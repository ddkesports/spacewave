package sobject_world_engine

import (
	"context"
	"sync/atomic"

	"github.com/s4wave/spacewave/db/block"
)

// WriteKind names what a published World commit stores.
type WriteKind int

const (
	// WriteKindWorld is the pages of the World the commit produced.
	WriteKindWorld WriteKind = iota
	// WriteKindPayload is the blocks the commit's operations carry.
	WriteKindPayload
	// WriteKindState is the SharedObject state record.
	WriteKindState
	// WriteKindReplay is the replay save: span blocks, base holds and the
	// cursor record.
	WriteKindReplay
	// writeKindCount is the number of write kinds.
	writeKindCount
)

// String returns the metric label of k, such as "world".
func (k WriteKind) String() string {
	return [...]string{"world", "payload", "state", "replay"}[k]
}

// CheckpointSkip names why the checkpointer skipped a checkpoint that was due.
type CheckpointSkip int

const (
	// CheckpointSkipPending is a checkpoint skipped while an admission awaits
	// its receipt.
	CheckpointSkipPending CheckpointSkip = iota
	// CheckpointSkipUnplaced is a checkpoint skipped because the replay holds
	// no World after the stable prefix.
	CheckpointSkipUnplaced
	// CheckpointSkipStale is a prepared checkpoint dropped because the held
	// state moved past its stable point.
	CheckpointSkipStale
	// checkpointSkipCount is the number of skip reasons.
	checkpointSkipCount
)

// String returns the metric label of s, such as "pending".
func (s CheckpointSkip) String() string {
	return [...]string{"pending", "unplaced", "stale"}[s]
}

// WriteFlows counts what the process's World engines stored since it started.
type WriteFlows struct {
	// Commits is the number of commits published atomically.
	Commits uint64
	// Bytes is the length stored by those commits, by WriteKind.
	Bytes [writeKindCount]uint64
	// Checkpoints is the number of stable checkpoints adopted.
	Checkpoints uint64
	// Skips is the number of due checkpoints skipped, by CheckpointSkip.
	Skips [checkpointSkipCount]uint64
}

// writeFlows holds the process's counters.
var writeFlows struct {
	commits     atomic.Uint64
	bytes       [writeKindCount]atomic.Uint64
	checkpoints atomic.Uint64
	skips       [checkpointSkipCount]atomic.Uint64
}

// GetWriteFlows returns the process's counters as of now.
func GetWriteFlows() WriteFlows {
	// Load each counter.
	var f WriteFlows
	f.Commits = writeFlows.commits.Load()
	for k := range f.Bytes {
		f.Bytes[k] = writeFlows.bytes[k].Load()
	}
	f.Checkpoints = writeFlows.checkpoints.Load()
	for s := range f.Skips {
		f.Skips[s] = writeFlows.skips[s].Load()
	}
	return f
}

// countSkip counts a due checkpoint skipped for reason s.
func countSkip(s CheckpointSkip) {
	writeFlows.skips[s].Add(1)
}

// countPublication counts a published commit whose block entries are batch,
// splitting them into the payloads, which payloads and the batch blocks they
// reach are, and the World pages, which the rest are, and its replay save
// entries.
func countPublication(batch, save []*block.PutBatchEntry, payloads []*block.BlockRef) {
	// Index the batch, then walk it from the payload roots.
	byRef := make(map[string]*block.PutBatchEntry, len(batch))
	for _, e := range batch {
		byRef[e.Ref.MarshalString()] = e
	}
	var payloadBytes uint64
	stack := payloads
	for len(stack) != 0 {
		ref := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		key := ref.MarshalString()
		e, ok := byRef[key]
		if !ok {
			continue
		}
		delete(byRef, key)
		payloadBytes += uint64(len(e.Data))
		stack = append(stack, e.Refs...)
	}

	// The batch blocks no payload reaches are World pages.
	var worldBytes uint64
	for _, e := range byRef {
		worldBytes += uint64(len(e.Data))
	}
	var saveBytes uint64
	for _, e := range save {
		saveBytes += uint64(len(e.Data))
	}
	writeFlows.commits.Add(1)
	writeFlows.bytes[WriteKindWorld].Add(worldBytes)
	writeFlows.bytes[WriteKindPayload].Add(payloadBytes)
	writeFlows.bytes[WriteKindReplay].Add(saveBytes)
}

// countHead returns head with the length of each record it writes counted as
// kind. The store calls Replace when it applies the publication.
func countHead(head *block.AtomicHeadUpdate, kind WriteKind) *block.AtomicHeadUpdate {
	counted := *head
	counted.Replace = func(ctx context.Context, current []byte, found bool) ([]byte, error) {
		next, err := head.Replace(ctx, current, found)
		if err == nil {
			writeFlows.bytes[kind].Add(uint64(len(next)))
		}
		return next, err
	}
	return &counted
}
