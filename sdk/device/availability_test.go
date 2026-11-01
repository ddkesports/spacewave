//go:build !tinygo

package s4wave_device_test

import (
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/s4wave/spacewave/core/transport"
	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/world"
	"github.com/s4wave/spacewave/net/crypto"
	"github.com/s4wave/spacewave/net/link"
	"github.com/s4wave/spacewave/net/peer"
	s4wave_device "github.com/s4wave/spacewave/sdk/device"
	"github.com/s4wave/spacewave/testbed"
)

// TestDeviceAvailabilityFollowsAuthenticatedTransport runs two authenticated
// peers against one Space World and keeps a watch across loss and reconnect.
func TestDeviceAvailabilityFollowsAuthenticatedTransport(t *testing.T) {
	// Start one Space World with a bounded test lifetime.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	tb, err := testbed.Default(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Release()

	// Give the observing Session and the Device independent persistent identities.
	observerKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	deviceKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	devicePeer, err := peer.IDFromPrivateKey(deviceKey)
	if err != nil {
		t.Fatal(err)
	}

	// Save the Device once, deliberately retaining a stale Online report.
	const objectKey = "devices/transport-test"
	device := &s4wave_device.Device{
		PeerId: devicePeer.String(), Label: "Build Host",
		SetupState: s4wave_device.DeviceSetupState_DEVICE_SETUP_STATE_DEVICE_SESSION_READY,
		LastStatus: &s4wave_device.DeviceStatus{Liveness: s4wave_device.DeviceLiveness_DEVICE_LIVENESS_ONLINE},
	}
	tx, err := tb.Engine.NewTransaction(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Discard()
	obj, _, err := world.CreateWorldObject(ctx, tx, objectKey, func(cursor *block.Cursor) error {
		cursor.SetBlock(device, true)
		return nil
	})
	world.ReleaseObjectState(obj)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// A valid Device without an observing identity has unknown reachability.
	err = s4wave_device.WatchDeviceAvailability(ctx, tb.Bus, "", tb.WorldState, objectKey, func(value *s4wave_device.DeviceAvailability) error {
		if value.GetState() != s4wave_device.DeviceAvailabilityState_DEVICE_AVAILABILITY_STATE_UNKNOWN {
			t.Fatal("missing observing identity reported known reachability")
		}
		return io.EOF
	})
	if err != io.EOF {
		t.Fatalf("initial snapshot callback result: %v", err)
	}

	// Publish availability before the observing transport exists.
	observer, err := transport.NewSessionTransport(tb.Logger, tb.Bus, observerKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	updates := make(chan *s4wave_device.DeviceAvailability, 8)
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- s4wave_device.WatchDeviceAvailability(watchCtx, tb.Bus, observer.GetPeerID(), tb.WorldState, objectKey, func(value *s4wave_device.DeviceAvailability) error {
			select {
			case updates <- value:
				return nil
			case <-watchCtx.Done():
				return watchCtx.Err()
			}
		})
	}()
	awaitDeviceAvailability(t, ctx, updates, device, s4wave_device.DeviceAvailabilityState_DEVICE_AVAILABILITY_STATE_OFFLINE)

	// Start the observer and admit only this Device on its authenticated UDP link.
	observerDone := make(chan error, 1)
	go func() { observerDone <- observer.Execute(ctx) }()
	if err := observer.AwaitReady(ctx); err != nil {
		t.Fatal(err)
	}

	// Connect, disconnect and reconnect with the same Device key and World record.
	revision, err := tb.Engine.GetSeqno(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		// Listen on a transport whose loss the observing Session owns directly.
		linkCtx, dropLink := context.WithCancel(ctx)
		addr, err := observer.StartUDP(linkCtx, transport.UDPConfig{
			ListenAddr: "127.0.0.1:0", NamedOnly: true,
			Peers: map[peer.ID]string{devicePeer: ""},
		})
		if err != nil {
			dropLink()
			t.Fatal(err)
		}

		// Establish a fresh authenticated transport incarnation for the same Device.
		deviceCtx, stopDevice := context.WithCancel(ctx)
		remote, err := transport.NewSessionTransport(tb.Logger, nil, deviceKey, "", "")
		if err != nil {
			stopDevice()
			t.Fatal(err)
		}
		remoteDone := make(chan error, 1)
		go func() { remoteDone <- remote.Execute(deviceCtx) }()
		if err := remote.AwaitReady(ctx); err != nil {
			stopDevice()
			t.Fatal(err)
		}
		_, err = remote.StartUDP(deviceCtx, transport.UDPConfig{
			ListenAddr: "127.0.0.1:0", NamedOnly: true,
			Peers: map[peer.ID]string{observer.GetPeerID(): addr.String()},
		})
		if err != nil {
			stopDevice()
			t.Fatal(err)
		}

		// Demand a link through Bifrost's ordinary authenticated dial path.
		_, linkRef, err := remote.GetChildBus().AddDirective(deviceCtx, link.NewEstablishLinkWithPeer(devicePeer, observer.GetPeerID()), nil)
		if err != nil {
			stopDevice()
			t.Fatal(err)
		}
		awaitDeviceAvailability(t, ctx, updates, device, s4wave_device.DeviceAvailabilityState_DEVICE_AVAILABILITY_STATE_ONLINE)

		// Drop the live transport and demand Offline without a status write.
		dropLink()
		awaitDeviceAvailability(t, ctx, updates, device, s4wave_device.DeviceAvailabilityState_DEVICE_AVAILABILITY_STATE_OFFLINE)
		stopDevice()
		<-remoteDone
		linkRef.Release()
		currentRevision, err := tb.Engine.GetSeqno(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if currentRevision != revision {
			t.Fatalf("transport transition wrote World revision: %d -> %d", revision, currentRevision)
		}
	}

	// Release the watch while the Space stays open, then stop the observer.
	stopWatch()
	if err := <-watchDone; err != context.Canceled {
		t.Fatalf("watch shutdown: %v", err)
	}
	cancel()
	<-observerDone
}

// awaitDeviceAvailability requires each next emitted projection to preserve the
// durable Device and reach the expected transport state promptly.
func awaitDeviceAvailability(t *testing.T, ctx context.Context, updates <-chan *s4wave_device.DeviceAvailability, device *s4wave_device.Device, state s4wave_device.DeviceAvailabilityState) {
	// Bound the observed transition independently from the whole test.
	t.Helper()
	select {
	case update := <-updates:
		if !update.GetDevice().EqualVT(device) {
			t.Fatalf("availability changed the durable Device: %v", update.GetDevice())
		}
		if update.GetState() != state {
			t.Fatalf("availability = %v, want %v", update.GetState(), state)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("availability did not reach %v promptly", state)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
