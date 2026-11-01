package world

import (
	"iter"
	"maps"
	"slices"
)

// ChangeIndex holds a change feed's watchers by the names in their
// ChangeInterests, so a ChangeSet finds the watchers it may concern with one
// lookup per changed key and quad field, independent of the watcher count.
// Watchers whose interest names nothing are candidates for every change.
//
// A shared read set is indexed once by its own reads while a watcher or a held
// set that joined it holds it, and links to the held sets it joined. A change
// a set's reads name reaches the set's watchers and those of every held set
// that joined it, directly or through others. A watcher holding a set built
// from large ones thus subscribes and leaves at the cost of its own reads and
// the sets it joins, never of every key below them. The caller synchronizes
// access.
type ChangeIndex[T comparable] struct {
	// interests maps each watcher to its interest.
	interests map[T]*ChangeInterest
	// watchers indexes the watchers by their own names.
	watchers nameIndex[T]
	// sets indexes the held read sets by their own names.
	sets nameIndex[*heldSet[T]]
	// held maps each held read set to its entry.
	held map[*ReadSet]*heldSet[T]
}

// heldSet is a read set the index names while a watcher or another held set
// holds it. Its names freeze when it is first held: a shared set is finished.
type heldSet[T comparable] struct {
	// set is the read set.
	set *ReadSet
	// names are the set's own reads, as indexed in sets.
	names *ChangeInterest
	// joined are the held sets this set joined. A set whose own names need
	// every change holds none: it is a candidate for every change already.
	joined []*heldSet[T]
	// parents are the held sets that joined this one.
	parents map[*heldSet[T]]struct{}
	// watchers are the watchers whose interest holds the set.
	watchers map[T]struct{}
}

// NewChangeIndex constructs an empty ChangeIndex.
func NewChangeIndex[T comparable]() *ChangeIndex[T] {
	return &ChangeIndex[T]{
		interests: make(map[T]*ChangeInterest),
		watchers:  newNameIndex[T](),
		sets:      newNameIndex[*heldSet[T]](),
		held:      make(map[*ReadSet]*heldSet[T]),
	}
}

// Len returns the number of watchers.
func (x *ChangeIndex[T]) Len() int {
	return len(x.interests)
}

// All iterates over the watchers.
func (x *ChangeIndex[T]) All() iter.Seq[T] {
	return maps.Keys(x.interests)
}

// Add indexes watcher w by the names in interest, replacing an earlier entry.
// A nil interest is tested on every change.
func (x *ChangeIndex[T]) Add(w T, interest *ChangeInterest) {
	// Replace an earlier entry, then index the watcher's own names.
	x.Remove(w)
	x.interests[w] = interest
	x.watchers.add(w, interest)
	if interest == nil || interest.all {
		return
	}

	// Hold each shared set for w.
	for _, set := range interest.shared {
		x.hold(set).watchers[w] = struct{}{}
	}
}

// Remove drops watcher w from the index.
func (x *ChangeIndex[T]) Remove(w T) {
	// Find the names w was indexed by.
	interest, ok := x.interests[w]
	if !ok {
		return
	}
	delete(x.interests, w)
	x.watchers.remove(w, interest)
	if interest == nil || interest.all {
		return
	}

	// Release each shared set w held.
	for _, set := range interest.shared {
		if entry := x.held[set]; entry != nil {
			delete(entry.watchers, w)
			x.releaseUnheld(entry)
		}
	}
}

// Candidates returns the watchers whose interest may accept changes, each
// once, in two lists. accepted holds those a changed object key names, directly
// or through a held set: a ChangeInterest's filter accepts any change to a key
// it names, so these need no test. tested holds the rest that must be tested:
// those named only by a changed quad field, whose pattern may still miss, and
// those tested on every change, or every watcher for an unknown ChangeSet. A
// watcher in neither rejects changes.
func (x *ChangeIndex[T]) Candidates(changes *ChangeSet) (accepted, tested []T) {
	// An unknown ChangeSet reaches every watcher, to be tested.
	if changes.Unknown {
		return nil, slices.Collect(maps.Keys(x.interests))
	}

	// Gather the watchers and held sets the changed keys name, then those the
	// other names reach.
	sure, maybe := make(map[T]struct{}), make(map[T]struct{})
	x.watchers.collectKeys(changes, sure)
	x.watchers.collectOthers(changes, maybe)
	keyed, named := make(map[*heldSet[T]]struct{}), make(map[*heldSet[T]]struct{})
	x.sets.collectKeys(changes, keyed)
	x.sets.collectOthers(changes, named)

	// Add the watchers of each named set and of every set that joined one. A
	// set a changed key names is touched, and so is each set that joined it.
	x.reachWatchers(keyed, sure, nil)
	x.reachWatchers(named, maybe, keyed)

	// Test only the watchers no changed key names.
	for w := range maybe {
		if _, ok := sure[w]; !ok {
			tested = append(tested, w)
		}
	}
	return slices.Collect(maps.Keys(sure)), tested
}

// reachWatchers adds to found the watchers of each set in named and of every
// held set that joined one, directly or through others. A set in skip, and
// the sets that joined it, are left out: another pass reaches them. named
// gains every set reached.
func (x *ChangeIndex[T]) reachWatchers(named map[*heldSet[T]]struct{}, found map[T]struct{}, skip map[*heldSet[T]]struct{}) {
	// Start from the named sets another pass does not reach.
	pending := make([]*heldSet[T], 0, len(named))
	for entry := range named {
		if _, ok := skip[entry]; !ok {
			pending = append(pending, entry)
		}
	}

	// Walk up to every set that joined one, collecting their watchers.
	for len(pending) != 0 {
		entry := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		maps.Copy(found, entry.watchers)
		for parent := range entry.parents {
			_, seen := named[parent]
			_, skipped := skip[parent]
			if !seen && !skipped {
				named[parent] = struct{}{}
				pending = append(pending, parent)
			}
		}
	}
}

// hold returns the entry of set, indexing it by its own reads and holding
// the sets it joined when no watcher or set held it yet.
func (x *ChangeIndex[T]) hold(set *ReadSet) *heldSet[T] {
	// Reuse the entry of a held set.
	if entry := x.held[set]; entry != nil {
		return entry
	}

	// Index the set by its own reads.
	names := &ChangeInterest{}
	joined := set.nameReads(names)
	entry := &heldSet[T]{
		set:      set,
		names:    names,
		parents:  make(map[*heldSet[T]]struct{}),
		watchers: make(map[T]struct{}),
	}
	x.held[set] = entry
	x.sets.add(entry, names)
	if names.all {
		return entry
	}

	// Hold the sets it joined, each linked back to it.
	for _, other := range joined {
		child := x.hold(other)
		child.parents[entry] = struct{}{}
		entry.joined = append(entry.joined, child)
	}
	return entry
}

// releaseUnheld drops entry once no watcher or set holds it, and releases the
// sets it joined.
func (x *ChangeIndex[T]) releaseUnheld(entry *heldSet[T]) {
	// Keep a set still held.
	if len(entry.watchers) != 0 || len(entry.parents) != 0 {
		return
	}

	// Drop its names, then its hold on each set it joined.
	x.sets.remove(entry, entry.names)
	delete(x.held, entry.set)
	for _, child := range entry.joined {
		delete(child.parents, entry)
		x.releaseUnheld(child)
	}
}

// nameIndex holds entries by the object keys and quad fields their names
// list. Entries whose names are nil or marked as needing every change are
// collected for every change.
type nameIndex[T comparable] struct {
	// keys maps each named object key to its entries.
	keys map[string]nameEntries[T]
	// terms maps each named quad field to its entries.
	terms map[quadTerm]nameEntries[T]
	// all holds the entries collected for every change.
	all map[T]struct{}
}

// newNameIndex constructs an empty nameIndex.
func newNameIndex[T comparable]() nameIndex[T] {
	return nameIndex[T]{
		keys:  make(map[string]nameEntries[T]),
		terms: make(map[quadTerm]nameEntries[T]),
		all:   make(map[T]struct{}),
	}
}

// add indexes e by names.
func (x *nameIndex[T]) add(e T, names *ChangeInterest) {
	// An entry without names sees every change.
	if names == nil || names.all {
		x.all[e] = struct{}{}
		return
	}

	// Index the named keys and quad fields.
	for _, key := range names.keys {
		addIndexed(x.keys, key, e)
	}
	for _, term := range names.terms {
		addIndexed(x.terms, term, e)
	}
}

// remove drops e, indexed by names, and each name e alone held.
func (x *nameIndex[T]) remove(e T, names *ChangeInterest) {
	// An entry without names is held only in all.
	if names == nil || names.all {
		delete(x.all, e)
		return
	}

	// Drop e from each name.
	for _, key := range names.keys {
		removeIndexed(x.keys, key, e)
	}
	for _, term := range names.terms {
		removeIndexed(x.terms, term, e)
	}
}

// collectKeys adds to found the entries a changed object key names.
func (x *nameIndex[T]) collectKeys(changes *ChangeSet, found map[T]struct{}) {
	for _, key := range changes.Keys {
		if entries, ok := x.keys[key]; ok {
			entries.collect(found)
		}
	}
}

// collectOthers adds to found the entries a changed quad field names and
// those collected for every change.
func (x *nameIndex[T]) collectOthers(changes *ChangeSet, found map[T]struct{}) {
	maps.Copy(found, x.all)
	for _, q := range changes.Quads {
		for _, term := range quadTerms(q) {
			if entries, ok := x.terms[term]; ok {
				entries.collect(found)
			}
		}
	}
}

// nameEntries are the entries one name indexes. Most names index one entry,
// so the first is held inline and only the others take a map; an index of
// many names then costs no map per name.
type nameEntries[T comparable] struct {
	// first is an entry of the name; every name in an index has one.
	first T
	// rest holds the name's other entries, or nil.
	rest map[T]struct{}
}

// collect adds the entries to found.
func (n nameEntries[T]) collect(found map[T]struct{}) {
	found[n.first] = struct{}{}
	maps.Copy(found, n.rest)
}

// addIndexed adds w to the watchers of name.
func addIndexed[K comparable, T comparable](index map[K]nameEntries[T], name K, w T) {
	entries, ok := index[name]
	switch {
	case !ok:
		index[name] = nameEntries[T]{first: w}
	case entries.first == w:
	case entries.rest == nil:
		entries.rest = map[T]struct{}{w: {}}
		index[name] = entries
	default:
		entries.rest[w] = struct{}{}
	}
}

// removeIndexed removes w from the watchers of name, and name once empty.
func removeIndexed[K comparable, T comparable](index map[K]nameEntries[T], name K, w T) {
	entries, ok := index[name]
	if !ok {
		return
	}
	if entries.first != w {
		delete(entries.rest, w)
		return
	}
	for next := range entries.rest {
		delete(entries.rest, next)
		entries.first = next
		if len(entries.rest) == 0 {
			entries.rest = nil
		}
		index[name] = entries
		return
	}
	delete(index, name)
}
