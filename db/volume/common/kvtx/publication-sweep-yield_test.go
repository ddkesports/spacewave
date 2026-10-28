package kvtx

import (
	"fmt"
	"testing"
	"time"

	"github.com/s4wave/spacewave/db/block"
	block_gc "github.com/s4wave/spacewave/db/block/gc"
	block_mock "github.com/s4wave/spacewave/db/block/mock"
)

// unreferencedCandidates stores n blocks in v, marks each unreferenced and
// returns their nodes, as a candidate scan lists them.
func unreferencedCandidates(t *testing.T, v *Volume, n int) []string {
	// Store each block and queue it as unreferenced.
	t.Helper()
	ctx := t.Context()
	rg := v.GetRefGraph()
	nodes := make([]string, 0, n)
	for i := range n {
		ref, _, err := block.PutBlock(ctx, v, &block_mock.Example{Msg: fmt.Sprintf("candidate %d", i)})
		if err != nil {
			t.Fatal(err)
		}
		node := block_gc.BlockIRI(ref)
		if err := rg.ApplyRefBatch(ctx, []block_gc.RefEdge{{Subject: block_gc.NodeUnreferenced, Object: node}}, nil); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// TestSweepUnreferencedYieldsPerCandidate checks that a sweep of unreferenced
// blocks lets a waiting writer in after one candidate, not after a chunk: with
// a writer always waiting, each sweep transaction removes one block.
func TestSweepUnreferencedYieldsPerCandidate(t *testing.T) {
	// Queue candidates, then report a writer waiting for the store.
	v, raw := newPublicationTestVolume(t)
	ctx := t.Context()
	nodes := unreferencedCandidates(t, v, 8)
	raw.mu.Lock()
	raw.writersWaiting = true
	before := raw.commits
	raw.mu.Unlock()

	// Sweep every candidate.
	swept, err := v.SweepUnreferenced(ctx, v.GetRefGraph(), nodes)
	if err != nil || len(swept) != len(nodes) {
		t.Fatalf("swept %d of %d: %v", len(swept), len(nodes), err)
	}

	// Each transaction committed one candidate.
	raw.mu.Lock()
	commits := raw.commits - before
	raw.mu.Unlock()
	if commits != len(nodes) {
		t.Fatalf("sweep committed %d transactions for %d candidates, want one each", commits, len(nodes))
	}
}

// TestSweepUnreferencedBoundsWriterHold checks that a sweep whose reads are
// slow, as cold pages are, holds the writer for about one budget plus one
// candidate's reads, so the candidates go in several transactions instead of
// one long hold.
func TestSweepUnreferencedBoundsWriterHold(t *testing.T) {
	// Queue candidates.
	v, raw := newPublicationTestVolume(t)
	ctx := t.Context()
	nodes := unreferencedCandidates(t, v, 40)

	// Slow each read under the writer, as a cold page does, and take the
	// commit count.
	const readDelay = 2 * time.Millisecond
	raw.mu.Lock()
	raw.readDelay = readDelay
	raw.longestWrite = 0
	before := raw.commits
	raw.mu.Unlock()

	// Sweep every candidate.
	swept, err := v.SweepUnreferenced(ctx, v.GetRefGraph(), nodes)
	if err != nil || len(swept) != len(nodes) {
		t.Fatalf("swept %d of %d: %v", len(swept), len(nodes), err)
	}

	// No transaction held the writer for long, and the sweep took several.
	raw.mu.Lock()
	longest, commits := raw.longestWrite, raw.commits-before
	raw.mu.Unlock()
	if limit := 3 * sweepBudget; longest > limit {
		t.Fatalf("a sweep transaction held the writer %v, limit %v (%d transactions)", longest, limit, commits)
	}
	if commits < 2 {
		t.Fatalf("sweep of %d slow candidates committed %d transaction, want several", len(nodes), commits)
	}
}
