//go:build !tinygo

package block_store_s3

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	"github.com/s4wave/spacewave/db/kvtx"
)

// importBatchRows bounds the rows of one WritePackIndexRows write transaction.
const importBatchRows = 8192

// ReadPackIndexRows calls fn with every row of the pack index in store, in key
// order, from one read transaction, so the rows satisfy the index's invariants
// together. It leaves out the t/ rows, which are a cache a reader refills. fn
// must copy what it keeps.
func ReadPackIndexRows(ctx context.Context, store kvtx.Store, fn func(key, value []byte) error) error {
	// One transaction fixes the rows.
	tx, err := store.NewTransaction(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Discard()

	// Pass on each row but the cache.
	it := tx.Iterate(ctx, nil, true, false)
	defer it.Close()
	for it.Next() {
		key := it.Key()
		if bytes.HasPrefix(key, []byte(indexTailPrefix)) {
			continue
		}
		value, err := it.Value()
		if err != nil {
			return err
		}
		if err := fn(key, value); err != nil {
			return err
		}
	}
	return it.Err()
}

// WritePackIndexRows writes the rows next returns into store until it reports
// no more, as ReadPackIndexRows read them, in batched transactions. store holds
// no other writer. Rows arrive in key order, so the p/ rows that mark a pack
// indexed land after the h rows they vouch for, and an interrupted write can be
// run again from the start.
func WritePackIndexRows(ctx context.Context, store kvtx.Store, next func() (key, value []byte, ok bool, err error)) error {
	for more := true; more; {
		// Open one batch.
		tx, err := store.NewTransaction(ctx, true)
		if err != nil {
			return err
		}

		// Fill it.
		for n := 0; n < importBatchRows; n++ {
			var key, value []byte
			key, value, more, err = next()
			if err == nil && more {
				err = tx.Set(ctx, key, value)
			}
			if err != nil {
				tx.Discard()
				return errors.Wrap(err, "write pack index rows")
			}
			if !more {
				break
			}
		}

		// Make it durable.
		if err := tx.Commit(ctx); err != nil {
			tx.Discard()
			return err
		}
	}
	return nil
}
