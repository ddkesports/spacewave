package store_kvtx_badger

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	bdb "github.com/dgraph-io/badger/v4"
	"github.com/s4wave/spacewave/db/kvtx"
)

// Tx is a badger transaction.
type Tx struct {
	s          *Store
	txn        *bdb.Txn
	commitOnce atomic.Bool
	write      bool
	// wrote records a Set or Delete, so an empty write transaction skips the
	// flush.
	wrote bool
	mtx   sync.Mutex
	rel   bool
	iters map[*Iterator]struct{}
}

// NewTx constructs a new badger transaction.
func (s *Store) newTx(txn *bdb.Txn, write bool) *Tx {
	return &Tx{s: s, txn: txn, write: write}
}

// Get returns values for a key.
func (t *Tx) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	// Locate the requested key in the Badger transaction.
	if len(key) == 0 {
		return nil, false, kvtx.ErrEmptyKey
	}
	item, err := t.txn.Get(key)
	if err != nil {
		if err == bdb.ErrKeyNotFound {
			err = nil
		}
		return nil, false, mapTxError(err)
	}

	// Copy the Badger value before the transaction releases it.
	valb, err := item.ValueCopy(nil)
	if err != nil {
		return nil, false, err
	}
	return valb, true, nil
}

// Size returns the number of keys in the store, walking the keys without
// reading their values.
func (t *Tx) Size(ctx context.Context) (uint64, error) {
	var size uint64
	err := t.ScanPrefixKeys(ctx, nil, func(key []byte) error {
		size++
		return nil
	})
	return size, err
}

// Set sets the value of a key.
// This will not be committed until Commit is called.
func (t *Tx) Set(ctx context.Context, key, value []byte) error {
	// Require a key, and own the slices Badger keeps until the commit.
	if len(key) == 0 {
		return kvtx.ErrEmptyKey
	}
	if err := t.txn.Set(slices.Clone(key), slices.Clone(value)); err != nil {
		return mapTxError(err)
	}

	// Record the change for the commit's flush and the value log pass.
	t.wrote = true
	t.s.dirty.Store(true)
	return nil
}

// scan delivers each item with a prefix in sorted order. Bolt keeps a key valid
// for its transaction, so each key is a copy callers may retain.
func (t *Tx) scan(prefix []byte, values bool, cb func(key []byte, item *bdb.Item) error) error {
	// Open a Badger iterator for the transaction snapshot.
	opts := bdb.DefaultIteratorOptions
	opts.PrefetchValues = values
	opts.Prefix = prefix
	it := t.txn.NewIterator(opts)
	defer it.Close()

	// Deliver each matching Badger entry to the scan callback.
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		if err := cb(item.KeyCopy(nil), item); err != nil {
			return err
		}
	}
	return nil
}

// ScanPrefix iterates over keys with a prefix.
func (t *Tx) ScanPrefix(ctx context.Context, prefix []byte, cb func(key, value []byte) error) error {
	return t.scan(prefix, true, func(key []byte, item *bdb.Item) error {
		return item.Value(func(val []byte) error {
			return cb(key, val)
		})
	})
}

// ScanPrefixKeys iterates over keys with a prefix, leaving the values unread.
func (t *Tx) ScanPrefixKeys(ctx context.Context, prefix []byte, cb func(key []byte) error) error {
	return t.scan(prefix, false, func(key []byte, _ *bdb.Item) error {
		return cb(key)
	})
}

// Iterate returns an iterator with a given key prefix.
//
// Should always return non-nil, with error field filled if necessary.
// If sort, iterates in sorted order, reverse reverses the key iteration.
// The prefix is NOT clipped from the output keys.
// If !sort, reverse MAY have no effect.
func (t *Tx) Iterate(ctx context.Context, prefix []byte, sort, reverse bool) kvtx.Iterator {
	// Configure the Badger iterator traversal direction.
	opts := bdb.DefaultIteratorOptions
	opts.Reverse = reverse

	// Badger does not bound reverse iteration by the prefix and reports an
	// out-of-prefix item as invalid without a way to step off it, so the
	// Iterator enforces the prefix itself when reversed.
	if !reverse {
		opts.Prefix = prefix
	}
	opts.AllVersions = false

	// Track the iterator under the transaction lock until it closes.
	t.mtx.Lock()
	rel := t.rel
	var it *Iterator
	if !rel {
		// createdIt holds the concrete iterator so the release func
		// removes its own map entry; reassigning the outer variable
		// would delete nothing.
		var createdIt *Iterator
		it = NewIterator(t.txn.NewIterator(opts), reverse, prefix, func() {
			t.mtx.Lock()
			if t.iters != nil {
				delete(t.iters, createdIt)
			}
			t.mtx.Unlock()
		})
		createdIt = it
		if t.iters == nil {
			t.iters = make(map[*Iterator]struct{})
		}
		t.iters[createdIt] = struct{}{}
	}
	t.mtx.Unlock()

	if it == nil {
		return kvtx.NewErrIterator(kvtx.ErrDiscarded)
	}
	return it
}

// Delete deletes a key.
// This will not be committed until Commit is called.
// Not found should not return an error.
func (t *Tx) Delete(ctx context.Context, key []byte) error {
	// Require a key, and own the slice Badger keeps until the commit.
	if len(key) == 0 {
		return kvtx.ErrEmptyKey
	}
	if err := t.txn.Delete(slices.Clone(key)); err != nil {
		return mapTxError(err)
	}

	// Record the change for the commit's flush and the value log pass.
	t.wrote = true
	t.s.dirty.Store(true)
	return nil
}

// mapTxError maps Badger transaction failures to their kvtx errors.
func mapTxError(err error) error {
	switch {
	case errors.Is(err, bdb.ErrTxnTooBig):
		return kvtx.ErrTxTooBig
	case errors.Is(err, bdb.ErrReadOnlyTxn):
		return kvtx.ErrNotWrite
	case errors.Is(err, bdb.ErrDiscardedTxn):
		return kvtx.ErrDiscarded
	case errors.Is(err, bdb.ErrConflict):
		return errors.Join(kvtx.ErrInvalidSnapshot, err)
	}
	return err
}

// Commit commits the transaction to storage and waits for its durability
// fence, unless ctx allows an ordered commit.
// Can return an error to indicate tx failure.
// Will return error if called after Discard()
func (t *Tx) Commit(ctx context.Context) error {
	// Apply the transaction in commit order.
	if err := t.CommitOrdered(ctx); err != nil {
		return err
	}

	// Fence the commit unless it changed nothing or may wait for a later one.
	if !t.wrote || t.s.noSync || kvtx.IsOrderedCommitContext(ctx) {
		return nil
	}
	return t.s.Sync(ctx)
}

// CommitOrdered commits the transaction with write ordering only. It becomes
// durable with the store's next flush.
func (t *Tx) CommitOrdered(ctx context.Context) error {
	// Refuse a finished transaction, and close the iterators Badger requires
	// closed before it commits.
	if !t.commitOnce.CompareAndSwap(false, true) {
		return kvtx.ErrDiscarded
	}
	t.closeIters()

	// Apply the transaction, counting it before the next writer can start so
	// the counts stay in commit order.
	if t.write {
		defer t.s.writeMtx.Unlock()
	}
	if err := t.txn.Commit(); err != nil {
		return mapTxError(err)
	}
	if t.wrote {
		t.s.markOrdered()
	}
	return nil
}

// closeIters closes the transaction's open iterators and refuses new ones.
func (t *Tx) closeIters() {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.rel = true
	for it := range t.iters {
		it.it.Close()
	}
	t.iters = nil
}

// Exists checks if a key exists.
func (t *Tx) Exists(ctx context.Context, key []byte) (bool, error) {
	if len(key) == 0 {
		return false, kvtx.ErrEmptyKey
	}
	_, err := t.txn.Get(key)
	if err == bdb.ErrKeyNotFound {
		return false, nil
	}
	return err == nil, mapTxError(err)
}

// Discard cancels the transaction.
// If called after Commit, does nothing.
// Cannot return an error.
// Can be called unlimited times.
func (t *Tx) Discard() {
	if t.commitOnce.CompareAndSwap(false, true) {
		t.closeIters()
		if t.write {
			defer t.s.writeMtx.Unlock()
		}
	}
	t.txn.Discard()
}

// _ is a type assertion
var (
	_ kvtx.Tx              = (*Tx)(nil)
	_ kvtx.OrderedCommitTx = (*Tx)(nil)
)
