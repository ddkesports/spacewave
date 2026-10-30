package block

import "sync"

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
}

// NewReplacementLog starts a log whose first interval begins at the root from.
func NewReplacementLog(from *BlockRef) *ReplacementLog {
	l := &ReplacementLog{}
	l.reset(from)
	return l
}

// Take returns what the writes since the interval began did to the stored
// pages under its root, with the root now at to, and starts the next interval
// at to.
func (l *ReplacementLog) Take(to *BlockRef) *Replacements {
	// Net the interval and start the next one under the lock.
	l.mtx.Lock()
	defer l.mtx.Unlock()
	out := l.net(to)
	l.reset(to)
	return out
}

// reset starts an empty interval at the root from. Assumes mtx is locked.
func (l *ReplacementLog) reset(from *BlockRef) {
	l.from = from.Clone()
	l.replaced = make(map[string]pageBase)
	l.created = make(map[string]pageBase)
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
// The live pages are those to reaches through the pages the interval created
// or replaced; a page outside both maps is a stored subtree the interval did
// not touch, live as a whole. The candidates are the old root, every replaced
// page and every page one named. A candidate not live is dead: a replaced or
// created page has its children accounted, any other heads a dead subtree.
func (l *ReplacementLog) net(to *BlockRef) *Replacements {
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
