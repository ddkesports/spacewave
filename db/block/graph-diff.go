package block

import "context"

// DiffGraph visits once each block the old roots reach that the new roots do
// not, reading child lists from src. It holds the set of blocks the new roots
// reach, so its memory grows with the new tree: it checks a transaction's
// Replacements in tests and serves a store that has no record of them.
//
// Returns the errors of WalkGraph, such as ErrNotFound at a hole.
func DiffGraph(ctx context.Context, src StoreOps, oldRoots, newRoots []*BlockRef, visit func(ref *BlockRef) error) error {
	// Collect every block the new roots reach.
	live := make(map[string]struct{})
	err := WalkGraph(ctx, src, newRoots, 0, func(ref *BlockRef, _ *StoredBlock) error {
		live[ref.MarshalString()] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}

	// Walk the old roots, stopping at each block the new roots reach.
	return WalkGraph(ctx, src, oldRoots, 0, func(ref *BlockRef, _ *StoredBlock) error {
		if _, ok := live[ref.MarshalString()]; ok {
			return ErrSkipRefs
		}
		return visit(ref)
	})
}
