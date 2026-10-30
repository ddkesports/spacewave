package block_mock

import (
	"context"

	"github.com/pkg/errors"
	"github.com/s4wave/spacewave/db/block"
)

// CheckReplacements compares a transaction's Replacements for a write from the
// root from to the root to against the diff of the two trees in src. It
// returns an error naming a page the diff finds dead that the record misses,
// or a page the record lists that the new tree still reaches. The record may
// also list pages the interval created and dropped, which neither tree reaches.
func CheckReplacements(ctx context.Context, src block.StoreOps, from, to *block.BlockRef, rep *block.Replacements) error {
	// A recording transaction always returns a record.
	if rep == nil {
		return errors.New("transaction recorded no replacements")
	}

	// Collect what the record reports dead.
	dead := make(map[string]struct{})
	for _, ref := range rep.Replaced {
		dead[ref.MarshalString()] = struct{}{}
	}
	err := rep.Expand(ctx, src, func(ref *block.BlockRef) error {
		dead[ref.MarshalString()] = struct{}{}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "expand removed subtrees")
	}

	// Every page the diff finds dead must be reported.
	err = block.DiffGraph(ctx, src, []*block.BlockRef{from}, []*block.BlockRef{to}, func(ref *block.BlockRef) error {
		if _, ok := dead[ref.MarshalString()]; !ok {
			return errors.Errorf("dead page %s not reported", ref.MarshalString())
		}
		return nil
	})
	if err != nil {
		return err
	}

	// No reported page may be live.
	return block.WalkGraph(ctx, src, []*block.BlockRef{to}, 0, func(ref *block.BlockRef, _ *block.StoredBlock) error {
		if _, ok := dead[ref.MarshalString()]; ok {
			return errors.Errorf("live page %s reported dead", ref.MarshalString())
		}
		return nil
	})
}
