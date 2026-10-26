package storebench

import (
	"context"
	"math/rand/v2"
	"strconv"

	"github.com/pkg/errors"
	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/block/byteslice"
	"github.com/s4wave/spacewave/db/world"
	"github.com/sirupsen/logrus"
)

// seedLinkPredicate is the graph predicate of the seeded links.
const seedLinkPredicate = "seed/link"

// Seed writes a synthetic World of conf.Objects objects to eng, each with a
// body and graph links to earlier objects, in writes of conf.Batch objects.
// It stands in for a copied server store while none is available.
func Seed(ctx context.Context, le *logrus.Entry, eng world.Engine, conf *SeedConfig) error {
	// Write the objects batch by batch.
	rng := rand.New(rand.NewPCG(conf.Seed, 0))
	batch := max(conf.Batch, 1)
	for start := 0; start < conf.Objects; start += batch {
		end := min(start+batch, conf.Objects)
		if err := seedBatch(ctx, eng, rng, conf, start, end); err != nil {
			return err
		}
		le.WithField("objects", end).Debug("seeded")
	}
	le.WithField("objects", conf.Objects).Info("seeded the World")
	return nil
}

// seedBatch creates objects start up to end in one World write.
func seedBatch(ctx context.Context, eng world.Engine, rng *rand.Rand, conf *SeedConfig, start, end int) error {
	// Open the write.
	tx, err := eng.NewTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Discard()

	// Create each object with its body and its links to earlier objects.
	for i := start; i < end; i++ {
		key := seedKey(i)
		body := make([]byte, conf.BodyBytes)
		copy(body, key)
		_, _, err := world.AccessWorldObject(ctx, tx, key, true, func(bcs *block.Cursor) error {
			bcs.SetBlock(byteslice.NewByteSlice(&body), true)
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "create "+key)
		}
		for range conf.Links {
			if i == 0 {
				break
			}
			q := world.NewGraphQuadWithKeys(key, seedLinkPredicate, seedKey(rng.IntN(i)), "")
			if err := tx.SetGraphQuad(ctx, q); err != nil {
				return errors.Wrap(err, "link "+key)
			}
		}
	}
	return tx.Commit(ctx)
}

// seedKey returns the key of seeded object i.
func seedKey(i int) string {
	return "seed/obj-" + strconv.Itoa(i)
}
