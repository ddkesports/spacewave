package sobject_world_engine

import (
	"bytes"
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/s4wave/spacewave/core/sobject"
	"github.com/s4wave/spacewave/db/tx"
	"github.com/s4wave/spacewave/net/crypto"
	"github.com/s4wave/spacewave/net/peer"
)

// TestFollowsWhileHandingOver checks that a leader handing over follows the
// checkpoints instead of replaying, refuses writes and stops preparing
// checkpoints, while the same leader in settled ownership replays and writes.
func TestFollowsWhileHandingOver(t *testing.T) {
	// The leader departs to a successor that already owns.
	ctx := t.Context()
	leader, leaderKey := newOwnershipTestPeer(t)
	successor, _ := newOwnershipTestPeer(t)
	cfg := &sobject.SharedObjectConfig{
		Participants: []*sobject.SOParticipantConfig{
			{PeerId: leader.String(), Role: sobject.SOParticipantRole_SOParticipantRole_OWNER},
			{PeerId: successor.String(), Role: sobject.SOParticipantRole_SOParticipantRole_OWNER},
		},
		Sequencer:       &sobject.SOSequencer{PeerId: leader.String()},
		ConfigChainHash: bytes.Repeat([]byte{1}, 32),
	}
	request, err := sobject.BuildSOLeaveRequest("test-object", cfg.GetConfigChainHash(), leaderKey)
	if err != nil {
		t.Fatal(err)
	}
	transfer := &sobject.SOConfigChange{
		ChangeType:   sobject.SOConfigChangeType_SO_CONFIG_CHANGE_TYPE_TRANSFER_OWNERSHIP,
		LeaveRequest: request,
	}

	// In settled ownership the leader replays and writes.
	settled := &ownershipTestSnapshot{cfg: cfg}
	settled.participant = cfg.GetParticipants()[0]
	e := &soEngine{so: &ownershipTestHost{testSharedObject: &testSharedObject{peerID: leader}, priv: leaderKey}}
	if follows, err := e.follows(ctx, settled); err != nil || follows {
		t.Fatalf("settled leader follows = %v, %v; want replay", follows, err)
	}
	if err := e.checkWritesOps(ctx, settled); err != nil {
		t.Fatalf("settled leader writes: %v", err)
	}

	// Handing over, it follows and writes nothing.
	handover := &ownershipTestSnapshot{cfg: cfg.CloneVT(), head: transfer}
	handover.cfg.ConfigChainHash = bytes.Repeat([]byte{2}, 32)
	handover.participant = handover.cfg.GetParticipants()[0]
	if follows, err := e.follows(ctx, handover); err != nil || !follows {
		t.Fatalf("departing leader follows = %v, %v; want follow", follows, err)
	}
	if err := e.checkWritesOps(ctx, handover); !errors.Is(err, tx.ErrNotWrite) {
		t.Fatalf("departing leader write check = %v; want ErrNotWrite", err)
	}

	// Nor does it prepare a checkpoint.
	set, err := handover.GetOperationSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt, err := e.prepareStableCheckpoint(ctx, handover, set); err != nil || ckpt != nil {
		t.Fatalf("departing leader prepared checkpoint %v: %v", ckpt, err)
	}
}

// ownershipTestSnapshot is a snapshot with a fixed config and config head
// change.
type ownershipTestSnapshot struct {
	testSharedObjectSnapshot
	// cfg is the config the snapshot holds.
	cfg *sobject.SharedObjectConfig
	// head is the change that produced cfg, or nil.
	head *sobject.SOConfigChange
}

// GetConfig returns the fixed config.
func (s *ownershipTestSnapshot) GetConfig(ctx context.Context) (*sobject.SharedObjectConfig, error) {
	return s.cfg, nil
}

// GetOwnership derives ownership from the fixed config and head change.
func (s *ownershipTestSnapshot) GetOwnership(ctx context.Context) (*sobject.SOOwnership, error) {
	return sobject.DeriveSOOwnership(s.cfg, s.head)
}

// ownershipTestHost is a shared object the local peer can sign through.
type ownershipTestHost struct {
	*testSharedObject
	// priv is the local participant's key.
	priv crypto.PrivKey
}

// GetSOHost returns no host: a departing checkpointer stops before using it.
func (h *ownershipTestHost) GetSOHost() *sobject.SOHost {
	return nil
}

// GetPrivKey returns the local participant's key.
func (h *ownershipTestHost) GetPrivKey() crypto.PrivKey {
	return h.priv
}

// newOwnershipTestPeer returns a new peer id and its key.
func newOwnershipTestPeer(t *testing.T) (peer.ID, crypto.PrivKey) {
	// Generate the key and derive its id.
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return id, priv
}
