package block_gc

import (
	"context"
	"slices"
	"strings"

	"github.com/s4wave/spacewave/db/kvtx"
)

// ReachableWithout marks as Reachable does, except that it never walks the
// records of an owner whose kind, the name before its colon, is in kinds. A
// cut owner counts as reached, but what it names is reached only by another
// path, so the difference from Reachable's Live is what only owners of those
// kinds keep. With kinds it reads the forward records once to find the cut
// owners; it is meant for diagnosis, not for a pass.
func (g *RefGraph) ReachableWithout(ctx context.Context, kinds ...string) (*BlockMark, error) {
	// Collect the reached nodes in the mark's Live.
	reached := NewNodeSet()
	mark := &BlockMark{Live: reached}
	err := g.transaction(ctx, false, func(ctx context.Context, tx kvtx.Tx) error {
		// Find the owners of the cut kinds by the subjects of forward records.
		cut := NewNodeSet()
		if len(kinds) != 0 {
			prefix := graphKey(kindForward)
			err := tx.ScanPrefixKeys(ctx, prefix, func(key []byte) error {
				node, _, err := readNode(key[len(prefix):])
				if err != nil {
					return err
				}
				if kind, _, _ := strings.Cut(node, ":"); slices.Contains(kinds, kind) {
					cut.Add(node)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		// Seed the stack with the graph root and the registered roots that
		// are not cut.
		reached.Add(NodeGCRoot)
		stack := []namedNode{{node: NodeGCRoot}}
		prefix := graphKey(kindRoot)
		err := tx.ScanPrefixKeys(ctx, prefix, func(key []byte) error {
			node := lastNode(prefix, key)
			if node != NodeUnreferenced && reached.Add(node) && !cut.Has(node) {
				stack = append(stack, namedNode{node, NodeGCRoot})
			}
			return nil
		})
		if err != nil {
			return err
		}

		// Walk from them, reaching but not walking the cut owners.
		_, err = walkTx(ctx, tx, reached, stack, nil, cut, mark.noteUnknown, nil, nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return mark, nil
}
