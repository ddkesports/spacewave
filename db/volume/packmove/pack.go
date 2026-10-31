package packmove

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/s4wave/spacewave/db/block"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// minFormatVersion is the first volume format that keeps a child list for
// every block, which Pack reads for the references of each block.
const minFormatVersion = 4

// batchesAhead is the number of read batches waiting for the upload, so the
// next batch reads while the last uploads.
const batchesAhead = 2

// Pack writes the World's blocks of cfg.Source, in key order, as packs in
// cfg.Packer.
//
// Source is only read, one snapshot per pack, and may be shared with the
// process that writes it: a snapshot the writer outran is read again. A block
// whose data does not match its hash is in no pack and is recorded as bad. A
// block written behind the cursor while Pack runs is not packed; Switch finds
// it. A block's references come from the volume's reference graph; a block the
// graph does not track is packed without references and counted in
// PackProgress.Untracked.
//
// The log at cfg.LogPath records each pack's keys before it uploads and a
// commit after, so a killed run resumes after its last committed pack. A pack
// that uploaded but did not commit is found in the bucket and committed
// without uploading again.
func Pack(ctx context.Context, cfg PackConfig) (PackProgress, error) {
	// Check the volume's format.
	if err := requireFormat(ctx, cfg); err != nil {
		return PackProgress{}, err
	}
	if cfg.WorldStoreID == "" {
		return PackProgress{}, errors.New("pack needs the World block store ID")
	}

	// Find the blocks outside the World, which stay in the volume.
	outside, err := FindOutsideWorld(ctx, cfg.Source, cfg.Keys, cfg.WorldStoreID)
	if err != nil {
		return PackProgress{}, errors.Wrap(err, "find the blocks outside the World")
	}

	// Resume from the log.
	log, state, err := openPackLog(cfg.LogPath)
	if err != nil {
		return PackProgress{}, errors.Wrap(err, "open pack log")
	}
	defer func() { _ = log.close() }()
	run := &packRun{cfg: cfg, log: log, state: state}
	run.progress = PackProgress{
		Packs: state.packs, Blocks: state.blocks, Bytes: state.bytes, Bad: state.bad,
		Resumed: state.blocks, Outside: outside.Len(), OutsideBytes: outside.Bytes(),
	}
	if cfg.BytesPerSecond > 0 {
		run.limiter = rate.NewLimiter(rate.Limit(cfg.BytesPerSecond), int(min(cfg.BytesPerSecond, 1<<30))) //nolint:gosec // bounded above.
	}
	if err := run.resolvePending(ctx); err != nil {
		return run.progress, err
	}

	// Read the blocks after the log's last key.
	reader := &batchReader{src: cfg.Source, prefix: cfg.Keys.GetBlockFullPrefix(), after: run.state.lastKey, packBytes: cfg.PackBytes, outside: outside}
	if reader.packBytes <= 0 {
		reader.packBytes = DefaultPackBytes
	}

	// Read the next batch while the last uploads, and measure the keys outside
	// the block range beside them.
	batches := make(chan *packBatch, batchesAhead)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(batches)
		for {
			batch, err := reader.next(gctx)
			if err != nil || batch == nil {
				return err
			}
			select {
			case batches <- batch:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})
	g.Go(func() error {
		for batch := range batches {
			if err := run.upload(gctx, batch); err != nil {
				return err
			}
		}
		return nil
	})
	if !cfg.SkipRest {
		g.Go(func() error {
			_, err := MeasureRest(gctx, cfg.Source, cfg.Keys, run.setRest)
			return err
		})
	}
	err = g.Wait()
	return run.snapshot(), err
}

// packRun holds the state of one Pack.
type packRun struct {
	// cfg is the configuration.
	cfg PackConfig
	// log is the pack log.
	log *packLog
	// state is what the log held at the start.
	state logState
	// limiter paces the upload; nil does not limit.
	limiter *rate.Limiter

	// mtx guards progress, which the upload and the measure both update.
	mtx sync.Mutex
	// progress is the totals so far.
	progress PackProgress
}

// snapshot returns the totals so far.
func (r *packRun) snapshot() PackProgress {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.progress
}

// update changes the totals under the lock and reports them.
func (r *packRun) update(fn func(p *PackProgress)) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	fn(&r.progress)
	if r.cfg.Progress != nil {
		r.cfg.Progress(r.progress)
	}
}

// setRest records the running count of the keys outside the block range.
func (r *packRun) setRest(rest RestReport) {
	r.update(func(p *PackProgress) { p.Rest = rest })
}

// resolvePending finishes the batch a killed run opened and did not commit,
// when its pack is in the bucket, and otherwise leaves it for the reader to
// read again.
func (r *packRun) resolvePending(ctx context.Context) error {
	// Find the batch that opened without a commit.
	pending := r.state.pending
	if pending == nil {
		return nil
	}

	// Ask the bucket for the batch's packed blocks.
	var refs []*block.BlockRef
	for _, e := range pending.entries {
		if e.bad {
			continue
		}
		ref := &block.BlockRef{}
		if err := ref.UnmarshalVT(e.key); err != nil {
			return errors.Wrap(err, "decode pending block key")
		}
		refs = append(refs, ref)
	}
	if len(refs) != 0 {
		found, err := r.cfg.Packer.GetBlockExistsBatch(ctx, refs)
		if err != nil {
			return errors.Wrap(err, "check the pending pack")
		}
		for _, ok := range found {
			if !ok {
				return nil
			}
		}
	}

	// Commit the pack that uploaded.
	if err := r.log.commit(); err != nil {
		return err
	}
	r.state.commit(pending)
	r.count(pending, 0)
	return nil
}

// upload writes a batch as one pack, paced by the byte limit, between its
// pending and commit frames. A batch with no packed block only advances the
// log.
func (r *packRun) upload(ctx context.Context, batch *packBatch) error {
	// Wait for the batch's share of the byte limit.
	if err := r.wait(ctx, batch.bytes); err != nil {
		return err
	}

	// Record the batch, upload it, then commit the record.
	if err := r.log.open(batch.frame); err != nil {
		return err
	}
	if len(batch.puts) != 0 {
		if _, err := r.cfg.Packer.PutBlockBatch(ctx, batch.puts); err != nil {
			return errors.Wrap(err, "write pack")
		}
	}
	if err := r.log.commit(); err != nil {
		return err
	}
	r.count(&batch.frame, batch.untracked)
	return nil
}

// count adds a committed batch to the totals and reports them.
func (r *packRun) count(batch *frame, untracked uint64) {
	// Add the batch's blocks, bytes and rejects to the totals.
	packed := batch.packed()
	r.update(func(p *PackProgress) {
		// A batch of only rejected blocks writes no pack.
		if packed != 0 {
			p.Packs++
		}
		p.Blocks += packed
		p.Bad += uint64(len(batch.entries)) - packed
		p.Bytes += batch.bytes
		p.Untracked += untracked
	})
}

// wait blocks until the limiter allows n bytes, in steps of its burst.
func (r *packRun) wait(ctx context.Context, n uint64) error {
	if r.limiter == nil {
		return nil
	}
	for n > 0 {
		step := int(min(n, uint64(r.limiter.Burst()))) //nolint:gosec // bounded by the burst.
		if err := r.limiter.WaitN(ctx, step); err != nil {
			return err
		}
		n -= uint64(step) //nolint:gosec // step is positive.
	}
	return nil
}
