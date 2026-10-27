package sobject

import (
	"slices"
	"testing"

	"github.com/pkg/errors"
)

// TestReadSOOwnershipHandover proves that ownership reads handing over between a
// departing owner's transfer and its successor's commit, and settled on either
// side of it, and that the departing owner signs no other change meanwhile.
func TestReadSOOwnershipHandover(t *testing.T) {
	// Hold an owner and two writers.
	ctx := t.Context()
	peers := createMockPeers(t, 3)
	keys := mustPrivKeys(t, peers)
	initial, genesis := newTestSOState(t, peers)
	base := initial.GetConfig()
	host, state := newLeaveTestHost(t, base, genesis)
	*state = initial
	owner, successor := peers[0].GetPeerID().String(), peers[1].GetPeerID().String()

	// Before any transfer the owner owns in settled ownership.
	own := mustReadSOOwnership(t, host, state)
	if !own.Settled() || !own.Owns(owner) || len(own.Owners()) != 1 {
		t.Fatalf("initial ownership: %+v", own)
	}

	// The owner transfers with its departure.
	request, err := BuildSOLeaveRequest(mockSharedObjectID, base.GetConfigChainHash(), keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TransferSOOwnership(ctx, host, keys[0], "", request); err != nil {
		t.Fatal(err)
	}

	// The departing owner still signs but no longer owns, and hands over.
	own = mustReadSOOwnership(t, host, state)
	if own.State() != SOOwnershipHandingOver || own.Owns(owner) || !own.Departing(owner) || !own.Owns(successor) {
		t.Fatalf("handover ownership: %+v", own)
	}
	if !slices.Equal(own.Owners(), []string{successor}) || !slices.Contains(own.SigningOwners(), owner) {
		t.Fatalf("handover owners %v signing %v", own.Owners(), own.SigningOwners())
	}

	// A writer reads the same handover, so it routes to the successor.
	if mustReadSOOwnership(t, host, state).Owns(owner) {
		t.Fatal("ownership read for a writer names the departing owner")
	}

	// The departing owner signs no other change on the transfer head.
	writerLeave, err := BuildSOLeaveRequest(mockSharedObjectID, (*state).GetConfig().GetConfigChainHash(), keys[2])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LeaveSOParticipants(ctx, host, keys[0], writerLeave); !errors.Is(err, ErrHandingOver) {
		t.Fatalf("departing owner signed a leave: %v", err)
	}

	// The successor commits the departure and ownership settles.
	if done, err := CompleteSOOwnershipTransfer(ctx, host, keys[1], ""); !done || err != nil {
		t.Fatalf("successor did not complete the transfer: %v %v", done, err)
	}
	own = mustReadSOOwnership(t, host, state)
	if !own.Settled() || !own.Owns(successor) || own.Owns(owner) {
		t.Fatalf("settled ownership: %+v", own)
	}
}

// mustReadSOOwnership reads ownership of the host's current config.
func mustReadSOOwnership(t *testing.T, host *SOHost, state **SOState) *SOOwnership {
	t.Helper()
	own, err := ReadSOOwnership(t.Context(), (*state).GetConfig(), host.ReadConfigEntry)
	if err != nil {
		t.Fatal(err)
	}
	return own
}
