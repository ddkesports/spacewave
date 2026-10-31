package world

import (
	"context"
)

// AdmitCommitter is a write Tx whose commit can return once the engine admits
// the write, before it is durable.
type AdmitCommitter interface {
	// CommitAdmit commits the write and returns once the engine admitted it
	// and installed the World after it, which later writes fork from. Readers
	// see that World once it is durable. On error the write was not admitted,
	// or it failed before CommitAdmit returned. Call Release on the Admission.
	CommitAdmit(ctx context.Context) (Admission, error)
}

// Admission is an admitted write whose durable result may be pending.
type Admission interface {
	// Seqno returns the World seqno after the write.
	Seqno() uint64
	// WaitDurable waits until the write is durable and readers see it, and
	// returns Seqno. When the write fails, the engine installs the World
	// without it and the writes admitted after it, and WaitDurable returns
	// the seqno of that World with the error. The wait ends with the engine's
	// lifetime, not with ctx: the outcome is then unknown.
	WaitDurable(ctx context.Context) (uint64, error)
	// Release releases the write. Call it once, after WaitDurable or to stop
	// waiting for the result.
	Release()
}

// ReinstallNotifier is an engine that reports the World it installs after
// dropping failed writes.
type ReinstallNotifier interface {
	// OnReinstall calls fn with the World seqno each time the engine drops
	// failed writes and installs the World without them, and when a new
	// engine opens in their place. fn runs under the engine's writer lock,
	// before a later write is admitted; it must not open a World
	// transaction. The returned func unregisters fn.
	OnReinstall(fn func(seqno uint64)) func()
}

// CommitAdmit commits tx and returns its Admission. A Tx that cannot commit
// at admission commits in full and returns an Admission that is durable.
func CommitAdmit(ctx context.Context, tx Tx) (Admission, error) {
	if ac, ok := tx.(AdmitCommitter); ok {
		return ac.CommitAdmit(ctx)
	}
	seqno, err := tx.GetSeqno(ctx)
	if err != nil {
		tx.Discard()
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return DurableAdmission(seqno), nil
}

// DurableAdmission is the Admission of a write that is already durable at
// seqno.
type DurableAdmission uint64

// Seqno returns the World seqno after the write.
func (a DurableAdmission) Seqno() uint64 { return uint64(a) }

// WaitDurable returns Seqno.
func (a DurableAdmission) WaitDurable(context.Context) (uint64, error) { return uint64(a), nil }

// Release does nothing.
func (a DurableAdmission) Release() {}

// releaseAdmission calls release after the Admission's Release.
type releaseAdmission struct {
	Admission
	release func()
}

// Release releases the Admission, then calls release.
func (a *releaseAdmission) Release() {
	a.Admission.Release()
	a.release()
}

// _ is a type assertion
var _ Admission = DurableAdmission(0)
