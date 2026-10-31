package block

import (
	"context"
	"slices"
)

// Replacements lists the stored pages the writes a ReplacementLog recorded
// stopped naming in one interval, so the caller can learn what died without
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

// Then returns what died across r's interval followed by next's, the interval
// that starts at the root r's ended at. A page next keeps is live at its end,
// so it leaves the dead pages even when r reported it, as when a later write
// stores a page again or attaches a subtree r removed. Expansion stops at
// every page either interval replaced or kept; stopping early only leaks.
func (r *Replacements) Then(next *Replacements) *Replacements {
	// Collect the pages next keeps.
	live := make(map[string]struct{}, len(next.Kept))
	for _, ref := range next.Kept {
		live[ref.MarshalString()] = struct{}{}
	}
	dead := func(lists ...[]*BlockRef) []*BlockRef {
		var out []*BlockRef
		for _, list := range lists {
			for _, ref := range list {
				if _, ok := live[ref.MarshalString()]; !ok {
					out = append(out, ref)
				}
			}
		}
		return out
	}

	// Join the dead pages, dropping the live ones, and every stop.
	return &Replacements{
		Replaced: dead(r.Replaced, next.Replaced),
		Removed:  dead(r.Removed, next.Removed),
		Kept:     slices.Concat(r.Kept, next.Kept),
	}
}
