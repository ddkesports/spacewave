package block

import (
	"errors"
	"testing"
)

// TestTakeStoredCountsFoundPages commits a version that writes again a page
// another writer stored, under a parent that replaced the page's old holder,
// and checks the record keeps the found page's children live, holds only the
// found page, and marks only its write counted. The next version names N, which
// the first created, from a second parent, and the joined record holds both.
func TestTakeStoredCountsFoundPages(t *testing.T) {
	// Store the old version R0 -> A -> H, and F -> H, K from another writer.
	ctx := t.Context()
	inner := newCountStore(0)
	page := func(st StoreOps, name string, refs ...*BlockRef) *pageBase {
		t.Helper()
		ref, _, err := st.PutBlock(ctx, []byte(name), &PutOpts{Refs: refs, RefsKnown: true})
		if err != nil {
			t.Fatal(err)
		}
		return &pageBase{ref: ref, refs: refs}
	}
	h, k := page(inner, "H"), page(inner, "K")
	a := page(inner, "A", h.ref)
	r0 := page(inner, "R0", a.ref)
	page(inner, "F", h.ref, k.ref)

	// X is the writer's own page, which a drain already wrote below.
	buf := NewBufferedStoreWithSettings(ctx, inner, &BufferedStoreSettings{RecordWrites: true})
	page(buf, "X")
	if err := buf.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Write R' -> A', X and A' -> F, N, recording each write.
	log := NewReplacementLog(r0.ref)
	f, n, x := page(buf, "F", h.ref, k.ref), page(buf, "N"), page(buf, "X")
	a2 := page(buf, "A'", f.ref, n.ref)
	r2 := page(buf, "R'", a2.ref, x.ref)
	log.recordWrite(r0, r2)
	log.recordWrite(a, a2)
	for _, p := range []*pageBase{f, n, x} {
		log.recordWrite(nil, p)
	}
	rep := log.TakeStored(ctx, r2.ref, buf.MarkStored)

	// R0 and A died; H stays live under F, and only F gained a holder.
	if len(rep.Replaced) != 2 || len(rep.Removed) != 0 {
		t.Fatalf("replaced %v, removed %v", rep.Replaced, rep.Removed)
	}
	if len(rep.Held) != 1 || !rep.Held[0].EqualsRef(f.ref) {
		t.Fatalf("held %v, want F", rep.Held)
	}

	// Only F's write is counted, and a later uncounted write of it clears the
	// mark.
	counted := func() map[string]bool {
		// Borrow the pending entries and return them to the buffer.
		t.Helper()
		batch, err := buf.TakePending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer batch.Complete(errors.New("keep pending"))
		out := make(map[string]bool)
		for _, e := range batch.Entries {
			out[string(e.Data)] = e.Counted
		}
		return out
	}
	if got := counted(); !got["F"] || got["N"] || got["X"] || got["A'"] || got["R'"] {
		t.Fatalf("counted %v, want only F", got)
	}
	if _, err := buf.PutBlockBatch(ctx, []*PutBatchEntry{{Ref: f.ref, Data: []byte("F"), Refs: f.refs, RefsKnown: true}}); err != nil {
		t.Fatal(err)
	}
	if got := counted(); got["F"] {
		t.Fatal("an uncounted write left F counted")
	}

	// Store the first version.
	if err := buf.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Write R3 -> A', X, B and B -> N in a new buffer, recording each write.
	buf = NewBufferedStoreWithSettings(ctx, inner, &BufferedStoreSettings{RecordWrites: true})
	log = NewReplacementLog(r2.ref)
	n = page(buf, "N")
	b := page(buf, "B", n.ref)
	r3 := page(buf, "R3", a2.ref, x.ref, b.ref)

	// Record the writes and take the interval.
	log.recordWrite(r2, r3)
	log.recordWrite(nil, b)
	log.recordWrite(nil, n)
	next := log.TakeStored(ctx, r3.ref, buf.MarkStored)

	// N gained B as a second holder; across both versions F and N did.
	if len(next.Held) != 1 || !next.Held[0].EqualsRef(n.ref) {
		t.Fatalf("next held %v, want N", next.Held)
	}
	joined := rep.Then(next)
	if len(joined.Held) != 2 || len(joined.Replaced) != 3 || len(joined.Removed) != 0 {
		t.Fatalf("joined held %v, replaced %v, removed %v", joined.Held, joined.Replaced, joined.Removed)
	}
}
