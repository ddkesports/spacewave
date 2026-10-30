package block

import "context"

// Replacements lists the stored pages a transaction's writes stopped naming
// between two TakeReplacements calls, so the caller can learn what died without
// walking the graph.
//
// It assumes each page has one parent in the old tree. A page with a second
// parent, such as a block put twice or a subtree copied by reference, may be
// listed while the other parent still names it.
type Replacements struct {
	// Replaced lists dead pages whose own children are accounted here: a stored
	// page a write encoded again, or a page the interval created and dropped.
	Replaced []*BlockRef
	// Removed lists the roots of dead stored subtrees. Each page under one is
	// dead too, except below a page in Replaced or Kept.
	Removed []*BlockRef
	// Kept lists live pages a write attached under a new parent, such as a
	// moved subtree or a written page. Expanding Removed stops at them.
	Kept []*BlockRef
}

// Expand visits each page in Removed and each page under one, stopping at the
// pages in Replaced and Kept, reading child lists from src.
//
// Returns the errors of WalkGraph, such as ErrNotFound at a hole.
func (r *Replacements) Expand(ctx context.Context, src StoreOps, visit func(ref *BlockRef) error) error {
	stop := make(map[string]struct{}, len(r.Replaced)+len(r.Kept))
	for _, ref := range r.Replaced {
		stop[ref.MarshalString()] = struct{}{}
	}
	for _, ref := range r.Kept {
		stop[ref.MarshalString()] = struct{}{}
	}
	return WalkGraph(ctx, src, r.Removed, 0, func(ref *BlockRef, _ *StoredBlock) error {
		if _, ok := stop[ref.MarshalString()]; ok {
			return ErrSkipRefs
		}
		return visit(ref)
	})
}

// pageBase is a stored page and the refs it names.
type pageBase struct {
	ref  *BlockRef
	refs []*BlockRef
}

// replacementLog accumulates the pages a transaction's writes replaced and
// created between TakeReplacements calls. The transaction mutex guards it.
type replacementLog struct {
	// from is the transaction root when the interval began.
	from *BlockRef
	// replaced holds each stored page a write encoded again, by ref.
	replaced map[string]pageBase
	// created holds each page a write stored, by ref.
	created map[string]pageBase
}

// newReplacementLog starts an interval at the root from.
func newReplacementLog(from *BlockRef) *replacementLog {
	return &replacementLog{
		from:     from,
		replaced: make(map[string]pageBase),
		created:  make(map[string]pageBase),
	}
}

// RecordReplacements starts recording the stored pages the transaction's
// writes replace, for TakeReplacements. Positions loaded before the call are
// recorded as if written without a base, so call it before reading.
func (t *Transaction) RecordReplacements() {
	if t == nil {
		return
	}
	t.mtx.Lock()
	if t.replacements == nil {
		t.replacements = newReplacementLog(t.root.ref)
	}
	t.mtx.Unlock()
}

// TakeReplacements returns what the writes since RecordReplacements or the
// last call did to stored pages, and starts the next interval at the current
// root. Returns nil when the transaction does not record.
func (t *Transaction) TakeReplacements() *Replacements {
	// A nil transaction records nothing.
	if t == nil {
		return nil
	}

	// Swap the log for a fresh one starting at the current root.
	t.mtx.Lock()
	defer t.mtx.Unlock()
	log := t.replacements
	if log == nil {
		return nil
	}
	t.replacements = newReplacementLog(t.root.ref)
	return log.net(t.root.ref)
}

// recordWrite records a page a write encoded: base is the stored page the
// position held, or nil, and page is what the write stored, or nil for an
// empty block.
func (l *replacementLog) recordWrite(base, page *pageBase) {
	if base != nil && page != nil && base.ref.EqualsRef(page.ref) {
		return
	}
	if base != nil {
		l.replaced[base.ref.MarshalString()] = *base
	}
	if page != nil {
		l.created[page.ref.MarshalString()] = *page
	}
}

// net derives the interval's Replacements with the root now at to.
//
// The live pages are those to reaches through the pages the interval created
// or replaced; a page outside both maps is a stored subtree the interval did
// not touch, live as a whole. The candidates are the old root, every replaced
// page and every page one named. A candidate not live is dead: a replaced or
// created page has its children accounted, any other heads a dead subtree.
func (l *replacementLog) net(to *BlockRef) *Replacements {
	// Walk the live pages from the new root through the touched pages.
	live := make(map[string]*BlockRef)
	stk := []*BlockRef{to}
	for len(stk) != 0 {
		ref := stk[len(stk)-1]
		stk = stk[:len(stk)-1]
		if ref.GetEmpty() {
			continue
		}
		key := ref.MarshalString()
		if _, ok := live[key]; ok {
			continue
		}
		live[key] = ref
		if page, ok := l.created[key]; ok {
			stk = append(stk, page.refs...)
		} else if page, ok := l.replaced[key]; ok {
			stk = append(stk, page.refs...)
		}
	}

	// Gather the pages a replaced page named; under the one parent rule, an
	// expansion cannot reach them.
	named := make(map[string]*BlockRef)
	for _, page := range l.replaced {
		for _, ref := range page.refs {
			named[ref.MarshalString()] = ref
		}
	}

	// Sort the dead candidates by whether their children are accounted.
	out := &Replacements{}
	for key, page := range l.replaced {
		if _, ok := live[key]; !ok {
			out.Replaced = append(out.Replaced, page.ref)
		}
	}
	for key, page := range l.created {
		_, isLive := live[key]
		_, isReplaced := l.replaced[key]
		if !isLive && !isReplaced {
			out.Replaced = append(out.Replaced, page.ref)
		}
	}
	removed := make([]*BlockRef, 0, len(named)+1)
	for _, ref := range named {
		removed = append(removed, ref)
	}
	if !l.from.GetEmpty() {
		removed = append(removed, l.from)
	}
	seen := make(map[string]struct{}, len(removed))
	for _, ref := range removed {
		key := ref.MarshalString()
		_, isSeen := seen[key]
		_, isLive := live[key]
		_, isReplaced := l.replaced[key]
		_, isCreated := l.created[key]
		seen[key] = struct{}{}
		if !isSeen && !isLive && !isReplaced && !isCreated {
			out.Removed = append(out.Removed, ref)
		}
	}

	// Keep the live pages that no replaced page named: attached here from a
	// position outside the touched pages, or written by this interval. A
	// written page may equal a stored page inside a removed subtree.
	for key, ref := range live {
		if _, isNamed := named[key]; !isNamed {
			out.Kept = append(out.Kept, ref)
		}
	}
	return out
}
