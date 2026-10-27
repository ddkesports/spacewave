package sobject_replica

import (
	"github.com/s4wave/spacewave/core/sobject"
	sobject_world_engine "github.com/s4wave/spacewave/core/sobject/world/engine"
)

// Observation is what a replica node sees on its own Device in one pass.
type Observation struct {
	// LocalPeerID is the peer ID of the Device running the node.
	LocalPeerID string
	// Spec is the node's own spec.
	Spec *ReplicaSpec
	// PeerSpec is the spec the other replica node sent, nil before it arrives.
	PeerSpec *ReplicaSpec
	// Ownership is who owns the SharedObject, nil when this Device is not a
	// participant.
	Ownership *sobject.SOOwnership
	// DesiredOwnerParticipates reports whether the desired owner is a
	// participant of the SharedObject.
	DesiredOwnerParticipates bool
	// InviteReceived reports whether the owner's invite has arrived.
	InviteReceived bool
	// Backfill is the outcome of this Device's last finished recheck of its
	// copy, nil before one finishes.
	Backfill *sobject_world_engine.BackfillState
	// ProofAccepted reports whether this Device, as owner, verified the desired
	// owner's proof against a state it holds.
	ProofAccepted bool
}
