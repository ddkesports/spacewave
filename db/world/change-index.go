package world

import (
	"iter"
	"maps"
	"slices"
)

// ChangeIndex holds a change feed's watchers by the names in their
// ChangeInterests, so a ChangeSet finds the watchers it may concern with one
// lookup per changed key and quad field, independent of the watcher count.
// Watchers whose interest names nothing are candidates for every change. The
// caller synchronizes access.
type ChangeIndex[T comparable] struct {
	// interests maps each watcher to its interest.
	interests map[T]*ChangeInterest
	// keys maps each named object key to its watchers.
	keys map[string]map[T]struct{}
	// terms maps each named quad field to its watchers.
	terms map[quadTerm]map[T]struct{}
	// all holds the watchers tested on every change.
	all map[T]struct{}
}

// NewChangeIndex constructs an empty ChangeIndex.
func NewChangeIndex[T comparable]() *ChangeIndex[T] {
	return &ChangeIndex[T]{
		interests: make(map[T]*ChangeInterest),
		keys:      make(map[string]map[T]struct{}),
		terms:     make(map[quadTerm]map[T]struct{}),
		all:       make(map[T]struct{}),
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
	// Replace an earlier entry, then record the interest.
	x.Remove(w)
	x.interests[w] = interest

	// A watcher without names sees every change.
	if interest == nil || interest.all {
		x.all[w] = struct{}{}
		return
	}

	// Index the named keys and quad fields.
	for _, key := range interest.keys {
		addIndexed(x.keys, key, w)
	}
	for _, term := range interest.terms {
		addIndexed(x.terms, term, w)
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
	delete(x.all, w)
	if interest == nil || interest.all {
		return
	}

	// Drop w from each name, and each name w alone held.
	for _, key := range interest.keys {
		removeIndexed(x.keys, key, w)
	}
	for _, term := range interest.terms {
		removeIndexed(x.terms, term, w)
	}
}

// Candidates returns each watcher whose interest may accept changes once: the
// watchers named by a changed key or quad field and those tested on every
// change, or every watcher for an unknown ChangeSet. A watcher left out
// rejects changes.
func (x *ChangeIndex[T]) Candidates(changes *ChangeSet) []T {
	// An unknown ChangeSet reaches every watcher.
	if changes.Unknown {
		return slices.Collect(maps.Keys(x.interests))
	}

	// Gather the watchers named by the changed keys and quad fields.
	found := maps.Clone(x.all)
	for _, key := range changes.Keys {
		maps.Copy(found, x.keys[key])
	}
	for _, q := range changes.Quads {
		for _, term := range quadTerms(q) {
			maps.Copy(found, x.terms[term])
		}
	}
	return slices.Collect(maps.Keys(found))
}

// addIndexed adds w to the watchers of name.
func addIndexed[K comparable, T comparable](index map[K]map[T]struct{}, name K, w T) {
	set := index[name]
	if set == nil {
		set = make(map[T]struct{})
		index[name] = set
	}
	set[w] = struct{}{}
}

// removeIndexed removes w from the watchers of name, and name once empty.
func removeIndexed[K comparable, T comparable](index map[K]map[T]struct{}, name K, w T) {
	set := index[name]
	delete(set, w)
	if len(set) == 0 {
		delete(index, name)
	}
}
