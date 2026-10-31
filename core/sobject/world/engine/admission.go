package sobject_world_engine

import (
	"context"
	"slices"

	"github.com/s4wave/spacewave/core/sobject"
	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/bucket"
)

// writeAdmission is a publication whose receipt the engine has not settled: a
// local write's, or the engine's own hold and replay save. It keeps what was
// durable before the publication, which a failed receipt restores.
type writeAdmission struct {
	// receipt resolves with the publication's durable result. Its submitter
	// owns it; the engine only reads it.
	receipt *block.PublicationReceipt
	// localID is the local id of the admitted operation, or empty when the
	// SharedObject did not report it.
	localID string
	// base is the replay's checkpoint World when the write was admitted. The
	// saved span holds positions above it.
	base *InnerState
	// span and spanLen are the saved replay span before the publication.
	span    *block.BlockRef
	spanLen int
	// baseUnheld is set when no durable save held base before the
	// publication.
	baseUnheld bool
	// retained is the head held under the accepted World root before the
	// publication.
	retained *bucket.ObjectRef
	// retainedRoots are the retained roots held before the publication.
	retainedRoots []*RetainedRoot
	// seqno is the seqno of the World installed before the publication.
	seqno uint64
}

// admit records a publication admitted with receipt, then records its replay
// save and its World as held, so the writes after it fork from that World and
// chain their publications after it. seqno is the seqno of the installed
// World. A failed receipt is settled when it resolves, without waiting for
// its writer. The caller holds the writer lock and passes the World the
// publication holds under the accepted World root.
func (e *soEngine) admit(receipt *block.PublicationReceipt, held *bucket.ObjectRef, seqno uint64) {
	e.admissions = append(e.admissions, &writeAdmission{
		receipt:       receipt,
		base:          e.replay.base,
		span:          e.replay.span,
		spanLen:       e.replay.spanLen,
		baseUnheld:    e.replay.baseUnheld,
		retained:      e.retained,
		retainedRoots: e.retainedRoots,
		seqno:         seqno,
	})
	e.retained = held
	e.replay.saved()
	go e.watchReceipt(receipt)
}

// watchReceipt advances the durable head once receipt resolves, and
// reinstalls the World from before it when it failed.
func (e *soEngine) watchReceipt(receipt *block.PublicationReceipt) {
	select {
	case <-receipt.Done():
	case <-e.lifetime.Done():
		return
	}
	if receipt.Err() != nil {
		_ = e.reinstall(e.lifetime)
	}
	e.promoteDurable()
}

// lastReceipt returns the receipt of the last admitted publication, which the
// next publication follows, or nil when every admission settled. A failed
// receipt is returned too, so a write forked from its World fails with it. The
// caller holds the writer lock.
func (e *soEngine) lastReceipt() *block.PublicationReceipt {
	if len(e.admissions) == 0 {
		return nil
	}
	return e.admissions[len(e.admissions)-1].receipt
}

// settle drops the admissions whose receipts resolved, in admission order. A
// failed receipt fails every later admission through After, so settle drops
// them all, restores the replay save, the held head and the held retained
// roots from before the first, records their operations as failed and calls
// the reinstall hooks with the seqno of the World before the first, before a
// later write can be admitted. The caller holds the writer lock.
func (e *soEngine) settle() {
	for len(e.admissions) != 0 {
		// Stop at the first unresolved receipt.
		adm := e.admissions[0]
		select {
		case <-adm.receipt.Done():
		default:
			return
		}

		// A successful receipt makes its publication durable.
		if adm.receipt.Err() == nil {
			e.admissions = e.admissions[1:]
			continue
		}

		// A failed receipt takes its successors with it.
		for _, failed := range e.admissions {
			if failed.localID != "" {
				e.c.failed[failed.localID] = struct{}{}
			}
		}
		e.replay.unsave(adm.base, adm.span, adm.spanLen, adm.baseUnheld)
		e.retained, e.retainedRoots = adm.retained, adm.retainedRoots
		e.admissions = nil
		e.c.callReinstall(adm.seqno)
	}
}

// settled returns a channel that closes when the oldest unsettled receipt
// resolves, or nil when every admission settled. The caller holds the writer
// lock.
func (e *soEngine) settled() <-chan struct{} {
	if len(e.admissions) == 0 {
		return nil
	}
	return e.admissions[0].receipt.Done()
}

// pending reports whether an admission waits for its receipt. Direct writes of
// the replay save, the held roots and the SharedObject state wait until none
// does, since each would name blocks only the pending publications hold. The
// caller holds the writer lock.
func (e *soEngine) pending() bool {
	return len(e.admissions) != 0
}

// reinstall settles the failed publication, installs the World from before
// it and returns that World's seqno. The receipt's watcher calls it, and a
// write whose receipt failed calls it before returning, so the next attempt,
// which reruns at once, finds that World installed. A failure to reinstall is
// logged; the watcher installs the World at the next state change. It ends
// with the engine's lifetime, not the write's context.
func (e *soEngine) reinstall(ctx context.Context) uint64 {
	// Take the writer lock the failed write released.
	ctx, cancel := e.detach(ctx)
	defer cancel()
	unlockWriteMtx, err := e.c.writeMtx.Lock(ctx, "reinstall")
	if err != nil {
		return 0
	}
	defer unlockWriteMtx()

	// Replay the state without the failed operations.
	snap, err := e.admittedState(ctx)
	if err == nil {
		_, err = e.advance(ctx, snap, nil)
	}
	if err != nil && ctx.Err() == nil {
		e.c.le.WithError(err).Warn("failed to reinstall the World after a failed publication")
	}
	seqno, _ := e.admitted.GetSeqno(ctx)
	return seqno
}

// admittedState settles the resolved receipts and returns the admitted state,
// waiting for a snapshot without the operations of failed publications. The
// caller holds the writer lock.
func (e *soEngine) admittedState(ctx context.Context) (sobject.SharedObjectStateSnapshot, error) {
	// Settle first, so a failure just resolved is waited out.
	e.settle()
	ctr, rel, err := accessAdmittedState(ctx, e.so, nil)
	if err != nil {
		return nil, err
	}
	defer rel()

	// Wait for a current snapshot.
	return ctr.WaitValueWithValidator(ctx, func(snap sobject.SharedObjectStateSnapshot) (bool, error) {
		if snap == nil {
			return false, nil
		}
		stale, err := e.c.stale(ctx, e.so, snap)
		return !stale, err
	}, nil)
}

// stale reports whether snap of so holds an operation of a failed publication.
// The SharedObject publishes each snapshot after the state it shows, so a
// snapshot of the state before a failed receipt reset it can arrive after the
// reset. Every operation has a new local id, so a failed one never returns. The
// caller holds the writer lock.
func (c *Controller) stale(ctx context.Context, so sobject.SharedObject, snap sobject.SharedObjectStateSnapshot) (bool, error) {
	// Skip reading the set while nothing failed.
	if len(c.failed) == 0 {
		return false, nil
	}

	// Look for each failed operation.
	set, err := snap.GetOperationSet(ctx)
	if err != nil {
		return false, err
	}
	self := so.GetPeerID().String()
	for localID := range c.failed {
		if set.Find(self, localID) != nil {
			return true, nil
		}
	}
	return false, nil
}

// reinstallHook is a registered reinstall hook.
type reinstallHook struct {
	fn func(seqno uint64)
}

// OnReinstall calls fn with the World seqno when failed writes are dropped,
// before a later write is admitted, and when a new engine opens in this
// controller. fn runs under the writer lock.
func (e *soEngine) OnReinstall(fn func(seqno uint64)) func() {
	return e.c.onReinstall(fn)
}

// onReinstall registers fn until the returned func is called.
func (c *Controller) onReinstall(fn func(seqno uint64)) func() {
	h := &reinstallHook{fn: fn}
	c.reinstallMtx.Lock()
	c.reinstallHooks = append(slices.Clip(c.reinstallHooks), h)
	c.reinstallMtx.Unlock()
	return func() {
		c.reinstallMtx.Lock()
		c.reinstallHooks = slices.DeleteFunc(slices.Clone(c.reinstallHooks), func(r *reinstallHook) bool { return r == h })
		c.reinstallMtx.Unlock()
	}
}

// callReinstall calls the reinstall hooks with seqno. The caller holds the
// writer lock.
func (c *Controller) callReinstall(seqno uint64) {
	c.reinstallMtx.Lock()
	hooks := c.reinstallHooks
	c.reinstallMtx.Unlock()
	for _, h := range hooks {
		h.fn(seqno)
	}
}
