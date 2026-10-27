package sobject_world_engine

import (
	"bytes"
	"context"

	"github.com/s4wave/spacewave/core/sobject"
)

// ownershipOf returns the ownership of the config held by snap, reading the
// head change only when the head moved since the last read. The caller holds
// the writer lock.
func (e *soEngine) ownershipOf(ctx context.Context, snap sobject.SharedObjectStateSnapshot) (*sobject.SOOwnership, error) {
	// Reuse the ownership derived for an unchanged head.
	cfg, err := snap.GetConfig(ctx)
	if err != nil {
		return nil, err
	}
	head := cfg.GetConfigChainHash()
	if e.ownership != nil && len(head) != 0 && bytes.Equal(e.ownershipHead, head) {
		return e.ownership, nil
	}

	// Derive it from the snapshot and keep it for the head.
	own, err := snap.GetOwnership(ctx)
	if err != nil {
		return nil, err
	}
	e.ownership, e.ownershipHead = own, bytes.Clone(head)
	return own, nil
}
