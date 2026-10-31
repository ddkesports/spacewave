package kvtx

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/s4wave/spacewave/db/block"
	block_gc "github.com/s4wave/spacewave/db/block/gc"
	"github.com/sirupsen/logrus"
)

// rootOwnerKind is the owner kind of the named bucket roots, which a block
// pass keeps from its mark to find the due deadlist records.
var rootOwnerKind = strings.TrimSuffix(RootOwnerPrefix, ":")

// deadlistSampleSize bounds the live pages logged for one record.
const deadlistSampleSize = 8

// checkDueDeadlists checks each due deadlist record a bucket names under
// block.DeadlistDueRootPrefix against mark, which read every child list and
// kept the named roots' edges. A page the record lists as replaced or removed
// that the mark reached and no shared count holds is a deadlist error: it is
// counted and logged, and nothing frees by the record. Each checked record's
// name is released, so a later pass sweeps it. A record the store no longer holds is released
// unchecked; one that fails to read stays for the next pass.
func (v *Volume) checkDueDeadlists(ctx context.Context, mark *block_gc.BlockMark) error {
	for _, edge := range mark.OwnerEdges {
		// Find a due record.
		bucketID, name, ok := parseRootOwnerIRI(edge.Subject)
		if !ok || !strings.HasPrefix(name, block.DeadlistDueRootPrefix) {
			continue
		}
		ref, ok := block_gc.ParseBlockIRI(edge.Object)
		if !ok {
			continue
		}

		// Read it and count the listed pages the mark reached.
		_, dead, err := block.ReadDeadlist(ctx, v, ref)
		switch {
		case errors.Is(err, block.ErrNotFound):
			if le := v.logDeadlist(bucketID, name); le != nil {
				le.Warn("due deadlist record is missing, releasing it unchecked")
			}
		case err != nil:
			if le := v.logDeadlist(bucketID, name); le != nil {
				le.WithError(err).Warn("unable to read due deadlist record")
			}
			continue
		default:
			if err := v.countDeadlist(ctx, bucketID, name, mark, dead); err != nil {
				if le := v.logDeadlist(bucketID, name); le != nil {
					le.WithError(err).Warn("unable to read shared counts of due deadlist record")
				}
				continue
			}
		}

		// Release its name.
		err = v.withDirectAtomic(ctx, func(blocks block.StoreOps, rg *block_gc.RefGraph) error {
			return v.setBucketRoot(ctx, blocks, rg, bucketID, name, nil)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// countDeadlist counts a checked record and the pages it lists as dead that
// mark reached, logging a sample of them. A reached page with a shared count
// is held by another holder, not an error.
func (v *Volume) countDeadlist(ctx context.Context, bucketID, name string, mark *block_gc.BlockMark, dead *block.Replacements) error {
	// Find the listed pages the mark reached.
	var reached []string
	for _, page := range slices.Concat(dead.Replaced, dead.Removed) {
		if node := block_gc.BlockIRI(page); mark.Live.Has(node) {
			reached = append(reached, node)
		}
	}

	// Drop the reached pages another holder keeps.
	var live []string
	n := 0
	if len(reached) != 0 {
		err := (&transactionRefGraph{volume: v}).read(ctx, func(rg *block_gc.RefGraph) error {
			for _, node := range reached {
				holds, err := rg.GetHolds(ctx, node)
				if err != nil {
					return err
				}
				if holds != 0 {
					continue
				}
				if n++; len(live) < deadlistSampleSize {
					live = append(live, node)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Count them, logging any.
	v.deadlistsChecked.Add(1)
	if n == 0 {
		return nil
	}
	v.deadlistLivePages.Add(uint64(n))
	if le := v.logDeadlist(bucketID, name); le != nil {
		le.WithField("live-pages", n).
			WithField("sample", live).
			Error("due deadlist record lists pages the mark reached")
	}
	return nil
}

// logDeadlist returns the log entry for a due record, or nil when the volume
// has no logger.
func (v *Volume) logDeadlist(bucketID, name string) *logrus.Entry {
	if v.le == nil {
		return nil
	}
	return v.le.WithField("bucket-id", bucketID).WithField("deadlist", name)
}
