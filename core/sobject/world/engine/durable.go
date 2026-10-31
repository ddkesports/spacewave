package sobject_world_engine

import (
	"context"
	"sync"

	"github.com/aperturerobotics/util/broadcast"
	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/bucket"
)

// durableHead is the World readers see. Writers fork from the admitted World,
// which the engine installs at admission. Each install queues its World here,
// and the durable head installs it once the publication admitted before it,
// if any, succeeds, in install order. A World after a failed publication is
// dropped: the reinstalled World follows it.
type durableHead struct {
	// promoteMtx serializes the installs of the durable head.
	promoteMtx sync.Mutex
	// bcast guards the fields below and broadcasts when done advances.
	bcast broadcast.Broadcast
	// queue holds the Worlds not yet installed or dropped, in install order.
	queue []durableInstall
	// queued is the position of the last queued World.
	queued uint64
	// done is the position of the last World installed or dropped.
	done uint64
}

// durableInstall is a World the durable head installs after a receipt.
type durableInstall struct {
	// pos is the position of the install.
	pos uint64
	// after is the receipt of the last publication admitted before the World
	// was installed for writers, or nil when none was pending.
	after *block.PublicationReceipt
	// ref is the World head.
	ref *bucket.ObjectRef
}

// splitHeads reports whether readers and writers use separate block engines.
func (e *soEngine) splitHeads() bool {
	return e.admitted != e.bengine
}

// queueDurable queues ref for the durable head after the last admitted
// publication, installs what it can and returns the install's position. The
// caller holds the writer lock and installed ref for writers.
func (e *soEngine) queueDurable(ref *bucket.ObjectRef) uint64 {
	if !e.splitHeads() {
		return 0
	}
	after := e.lastReceipt()
	var pos uint64
	e.durable.bcast.HoldLock(func(_ func(), _ func() <-chan struct{}) {
		e.durable.queued++
		pos = e.durable.queued
		e.durable.queue = append(e.durable.queue, durableInstall{pos: pos, after: after, ref: ref})
	})
	e.promoteDurable()
	return pos
}

// durablePos returns the position of the last queued World.
func (e *soEngine) durablePos() uint64 {
	var pos uint64
	e.durable.bcast.HoldLock(func(_ func(), _ func() <-chan struct{}) {
		pos = e.durable.queued
	})
	return pos
}

// promoteDurable installs the queued Worlds whose receipts resolved, in
// order, dropping those after a failed receipt. It stops at the first
// unresolved receipt, or at an install error, which it logs; the next
// resolved receipt retries. It ends with the engine's lifetime.
func (e *soEngine) promoteDurable() {
	ctx := e.lifetime
	e.durable.promoteMtx.Lock()
	defer e.durable.promoteMtx.Unlock()
	for {
		// Take the oldest queued World.
		var next durableInstall
		var found bool
		e.durable.bcast.HoldLock(func(_ func(), _ func() <-chan struct{}) {
			if len(e.durable.queue) != 0 {
				next, found = e.durable.queue[0], true
			}
		})
		if !found {
			return
		}

		// Install it once its receipt succeeds, or drop it.
		if next.after != nil {
			select {
			case <-next.after.Done():
			default:
				return
			}
		}
		if next.after == nil || next.after.Err() == nil {
			if !e.bengine.GetRootRef().EqualVT(next.ref) {
				if err := e.bengine.SetRootRef(ctx, next.ref); err != nil {
					if ctx.Err() == nil {
						e.c.le.WithError(err).Warn("failed to install the durable World")
					}
					return
				}
			}
		}
		e.durable.bcast.HoldLock(func(broadcast func(), _ func() <-chan struct{}) {
			e.durable.queue = e.durable.queue[1:]
			e.durable.done = next.pos
			broadcast()
		})
	}
}

// waitDurable waits until the durable head installed or dropped the World at
// pos.
func (e *soEngine) waitDurable(ctx context.Context, pos uint64) error {
	e.promoteDurable()
	for {
		var wait <-chan struct{}
		e.durable.bcast.HoldLock(func(_ func(), getWaitCh func() <-chan struct{}) {
			if e.durable.done < pos {
				wait = getWaitCh()
			}
		})
		if wait == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-wait:
		}
	}
}
