package block

import (
	"context"
	"sync"
)

// pageBase is a stored page and the refs it names.
type pageBase struct {
	ref  *BlockRef
	refs []*BlockRef
}

// ReplacementLog gathers the stored pages that writes replaced and created
// between Take calls. Any number of transactions over one store may record into
// it, such as a World's own transaction and the object transactions it stages,
// so a page one writes and another names nets out. It is safe for concurrent
// use.
type ReplacementLog struct {
	// mtx guards the fields below.
	mtx sync.Mutex
	// from is the root when the interval began.
	from *BlockRef
	// replaced holds each stored page a write encoded again, by ref.
	replaced map[string]pageBase
	// created holds each page a write stored, by ref.
	created map[string]pageBase
	// found holds each written page the store already held, moved from
	// created by TakeStored, by ref.
	found map[string]pageBase
	// forfeit is set when the interval's record cannot be trusted.
	forfeit bool
}

// NewReplacementLog starts a log whose first interval begins at the root from.
func NewReplacementLog(from *BlockRef) *ReplacementLog {
	l := &ReplacementLog{}
	l.reset(from)
	return l
}

// From returns the root the current interval began at.
func (l *ReplacementLog) From() *BlockRef {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return l.from.Clone()
}

// Forfeit gives up the current interval's record, for a write the record
// cannot account for, such as one that gives a page a second parent the record
// does not count. Take then reports nothing for the interval, so what died in
// it leaks rather than a page another parent names being reported dead.
func (l *ReplacementLog) Forfeit() {
	l.mtx.Lock()
	l.forfeit = true
	l.mtx.Unlock()
}

// Take returns what the writes since the interval began did to the stored
// pages under its root, with the root now at to, and starts the next interval
// at to. Returns nil when the interval was forfeited.
func (l *ReplacementLog) Take(to *BlockRef) *Replacements {
	// Net the interval and start the next one under the lock.
	l.mtx.Lock()
	defer l.mtx.Unlock()
	var out *Replacements
	if !l.forfeit {
		out = l.net(to)
	}
	l.reset(to)
	return out
}

// TakeStored is Take for writes that may store a page another writer already
// stored. stored reports which of the given created pages the store already
// held and marks their writes counted, as BufferedStore.MarkStored does. Such
// a page is one the interval named rather than created, so the record counts
// it held where it gained a holder. When stored fails, the interval is
// forfeited.
//
// The probe runs under the log's lock: the writes of the interval are done
// when its root is taken.
func (l *ReplacementLog) TakeStored(ctx context.Context, to *BlockRef, stored func(context.Context, []*BlockRef) ([]bool, error)) *Replacements {
	// Probe the created pages and net the interval under the lock.
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if !l.forfeit && len(l.created) != 0 {
		l.moveStored(ctx, stored)
	}
	var out *Replacements
	if !l.forfeit {
		out = l.net(to)
	}
	l.reset(to)
	return out
}

// moveStored moves from created to found each page stored reports the store
// held, or forfeits the interval when stored fails. Assumes mtx is locked.
func (l *ReplacementLog) moveStored(ctx context.Context, stored func(context.Context, []*BlockRef) ([]bool, error)) {
	// Ask about every created page at once.
	keys := make([]string, 0, len(l.created))
	refs := make([]*BlockRef, 0, len(l.created))
	for key, page := range l.created {
		keys = append(keys, key)
		refs = append(refs, page.ref)
	}
	found, err := stored(ctx, refs)
	if err != nil {
		l.forfeit = true
		return
	}

	// The found pages were named, not created.
	for i, ok := range found {
		if ok {
			l.found[keys[i]] = l.created[keys[i]]
			delete(l.created, keys[i])
		}
	}
}

// reset starts an empty interval at the root from. Assumes mtx is locked.
func (l *ReplacementLog) reset(from *BlockRef) {
	// Start from the root with empty maps.
	l.from = from.Clone()
	l.replaced = make(map[string]pageBase)
	l.created = make(map[string]pageBase)
	l.found = make(map[string]pageBase)
	l.forfeit = false
}

// RecordReplacements makes the transaction's writes record the stored pages
// they replace into log. Positions loaded before the call are recorded as if
// written without a base, so call it before reading.
func (t *Transaction) RecordReplacements(log *ReplacementLog) {
	if t == nil {
		return
	}
	t.mtx.Lock()
	t.replacements = log
	t.mtx.Unlock()
}

// GetReplacementLog returns the log the transaction records into, or nil.
func (t *Transaction) GetReplacementLog() *ReplacementLog {
	if t == nil {
		return nil
	}
	t.mtx.Lock()
	defer t.mtx.Unlock()
	return t.replacements
}

// recordWrite records a page a write encoded: base is the stored page the
// position held, or nil, and page is what the write stored, or nil for an
// empty block.
func (l *ReplacementLog) recordWrite(base, page *pageBase) {
	// Re-encoding a page to the same ref changes nothing.
	if base != nil && page != nil && base.ref.EqualsRef(page.ref) {
		return
	}

	// Record the replaced base and the stored page.
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if base != nil {
		l.replaced[base.ref.MarshalString()] = *base
	}
	if page != nil {
		l.created[page.ref.MarshalString()] = *page
	}
}

// net derives the interval's Replacements with the root now at to. Assumes
// mtx is locked.
//
// The live pages are those to reaches through the pages the interval wrote:
// created, found or replaced. A page outside the three maps is a stored
// subtree the interval did not touch, live as a whole. The candidates are the
// old root, every replaced page and every page one named. A candidate not live
// is dead: a replaced or created page has its children accounted, any other
// heads a dead subtree.
//
// A live page outside the created and replaced maps that no replaced page
// named, and that is not the old root, is held when the new root or a created
// page names it: it gained a holder unless its old parent died untouched,
// which only applying the record can see. A found page's children gained no
// holder, since the stored copy already named them.
func (l *ReplacementLog) net(to *BlockRef) *Replacements {
	// Walk the live pages from the new root through the written pages,
	// marking each page the new root or a created page names.
	type step struct {
		ref    *BlockRef
		direct bool
	}
	live := make(map[string]*BlockRef)
	direct := make(map[string]struct{})
	stk := []step{{to, true}}
	for len(stk) != 0 {
		st := stk[len(stk)-1]
		stk = stk[:len(stk)-1]
		if st.ref.GetEmpty() {
			continue
		}
		key := st.ref.MarshalString()
		if st.direct {
			direct[key] = struct{}{}
		}
		if _, ok := live[key]; ok {
			continue
		}
		live[key] = st.ref
		if page, ok := l.created[key]; ok {
			for _, ref := range page.refs {
				stk = append(stk, step{ref, true})
			}
		} else if page, ok := l.replaced[key]; ok {
			for _, ref := range page.refs {
				stk = append(stk, step{ref, true})
			}
		} else if page, ok := l.found[key]; ok {
			for _, ref := range page.refs {
				stk = append(stk, step{ref, false})
			}
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
	from := l.from.MarshalString()
	for key, ref := range live {
		if _, isNamed := named[key]; isNamed {
			continue
		}
		out.Kept = append(out.Kept, ref)
		_, isDirect := direct[key]
		_, isReplaced := l.replaced[key]
		_, isCreated := l.created[key]
		if isDirect && !isReplaced && !isCreated && key != from {
			out.Held = append(out.Held, ref)
		}
	}
	return out
}
