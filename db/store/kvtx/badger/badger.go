package store_kvtx_badger

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aperturerobotics/util/broadcast"
	bdb "github.com/dgraph-io/badger/v4"
	"github.com/s4wave/spacewave/db/kvtx"
)

// SyncDeadline is how long an ordered commit may stay pending before the store
// flushes it on its own.
const SyncDeadline = time.Second

// valueLogGCInterval is how often the store reclaims dead value log space
// while writes or deletes have happened since the last pass.
const valueLogGCInterval = time.Minute

// valueLogGCDiscardRatio is the dead fraction of a value log file that makes
// rewriting it worthwhile.
const valueLogGCDiscardRatio = 0.5

// Store is a badger database key-value store.
//
// Badger commits without a flush, in commit order. Every write transaction
// therefore commits ordered, and a full Commit adds a Sync that covers it and
// every commit before it, so concurrent full commits share one flush. An
// ordered commit becomes durable at the first of: Sync, a later full commit,
// SyncDeadline after the first pending commit, or Close.
type Store struct {
	db *bdb.DB
	// noSync skips the flush of a full Commit, trading the durability of
	// commits for write speed. The deadline flush still runs.
	noSync bool
	// writeMtx holds one write transaction at a time. Application code expects
	// a single writer, so Badger's conflict detection stays off.
	writeMtx sync.Mutex
	// dirty records a write or delete since the last value log pass.
	dirty atomic.Bool

	// ordered counts completed write commits.
	ordered atomic.Uint64
	// durable is the highest ordered count a flush has made durable.
	durable atomic.Uint64
	// syncMtx serializes flushes.
	syncMtx sync.Mutex

	// bcast guards the fields below and broadcasts when durable advances or
	// the deadline flush fails.
	bcast broadcast.Broadcast
	// deadline flushes pending commits, nil while none is armed.
	deadline *time.Timer
	// closed stops arming the deadline.
	closed bool
	// syncErr is the last deadline flush failure, cleared by a flush.
	syncErr error
}

// NewStore constructs a new key-value store from a badger db.
func NewStore(db *bdb.DB) *Store {
	return &Store{db: db}
}

// Open opens a badger database store. The store fences commits itself, so
// Badger's per-write sync stays off; opts.SyncWrites set to false is not a
// request to skip the fence, which is what noSync asks for.
func Open(opts bdb.Options, noSync bool) (*Store, error) {
	b, err := bdb.Open(opts.WithSyncWrites(false))
	if err != nil {
		return nil, err
	}

	s := NewStore(b)
	s.noSync = noSync
	return s, nil
}

// GetDB returns the badger DB.
func (s *Store) GetDB() *bdb.DB {
	return s.db
}

// NewTransaction returns a new transaction against the store.
// Indicate write if the transaction will not be read-only.
// Always call Discard() after you are done with the transaction.
func (s *Store) NewTransaction(ctx context.Context, write bool) (kvtx.Tx, error) {
	if write {
		s.writeMtx.Lock()
	}
	return s.newTx(s.db.NewTransaction(write), write), nil
}

// Execute reclaims dead value log space until ctx ends. Without it an evicted
// value never leaves the disk. It has nothing to do with block collection.
// Returning nil ends execution.
// Returning an error triggers a retry with backoff.
func (s *Store) Execute(ctx context.Context) error {
	ticker := time.NewTicker(valueLogGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
		if !s.dirty.Swap(false) {
			continue
		}

		// Rewrite one file per pass until a pass finds nothing to reclaim.
		for ctx.Err() == nil {
			if err := s.db.RunValueLogGC(valueLogGCDiscardRatio); err != nil {
				break
			}
		}
	}
}

// Sync makes every completed commit durable. It flushes only when a commit is
// not yet covered by an earlier flush, and a caller queued behind a flush
// finds its commit covered.
func (s *Store) Sync(ctx context.Context) error {
	// Skip the flush when completed commits are already durable.
	if s.durable.Load() >= s.ordered.Load() {
		return nil
	}

	// Serialize the flush, reading the count inside so it covers every commit
	// completed while this caller waited.
	s.syncMtx.Lock()
	defer s.syncMtx.Unlock()
	ordered := s.ordered.Load()
	if s.durable.Load() >= ordered {
		return nil
	}
	if err := s.db.Sync(); err != nil {
		return err
	}
	s.markDurable(ordered)
	return nil
}

// WaitDurable waits until every commit completed before the call is durable,
// without forcing a flush. It returns the deadline flush error if that flush
// fails first.
func (s *Store) WaitDurable(ctx context.Context) error {
	target := s.ordered.Load()
	for {
		var waitCh <-chan struct{}
		var err error
		done := false
		s.bcast.HoldLock(func(_ func(), getWaitCh func() <-chan struct{}) {
			if s.durable.Load() >= target {
				done = true
				return
			}
			err = s.syncErr
			waitCh = getWaitCh()
		})
		if done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-waitCh:
		}
	}
}

// Close stops the deadline, flushes pending commits, and closes the database.
func (s *Store) Close() error {
	s.bcast.HoldLock(func(_ func(), _ func() <-chan struct{}) {
		s.closed = true
		if s.deadline != nil {
			s.deadline.Stop()
			s.deadline = nil
		}
	})
	return errors.Join(s.Sync(context.Background()), s.db.Close())
}

// markOrdered counts a completed commit and arms the deadline flush unless one
// is already armed.
func (s *Store) markOrdered() {
	s.ordered.Add(1)
	s.bcast.HoldLock(func(_ func(), _ func() <-chan struct{}) {
		if s.closed || s.deadline != nil {
			return
		}
		s.deadline = time.AfterFunc(SyncDeadline, s.flushDeadline)
	})
}

// flushDeadline flushes the commits pending when the deadline fired.
func (s *Store) flushDeadline() {
	s.bcast.HoldLock(func(_ func(), _ func() <-chan struct{}) {
		s.deadline = nil
	})
	err := s.Sync(context.Background())
	s.bcast.HoldLock(func(broadcast func(), _ func() <-chan struct{}) {
		if err != nil {
			s.syncErr = err
			broadcast()
		}
	})
}

// markDurable records that the first n commits are durable.
func (s *Store) markDurable(n uint64) {
	for {
		durable := s.durable.Load()
		if durable >= n {
			return
		}
		if s.durable.CompareAndSwap(durable, n) {
			break
		}
	}
	s.bcast.HoldLock(func(broadcast func(), _ func() <-chan struct{}) {
		s.syncErr = nil
		broadcast()
	})
}

// SupportsAtomicCommit reports whether Commit finishes with a durability fence.
func (s *Store) SupportsAtomicCommit() bool {
	return !s.noSync
}

// _ is a type assertion
var (
	_ kvtx.Store              = (*Store)(nil)
	_ kvtx.OrderedCommitStore = (*Store)(nil)
	_ kvtx.AtomicCommitStore  = (*Store)(nil)
)
