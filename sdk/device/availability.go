package s4wave_device

import (
	"context"

	"github.com/aperturerobotics/controllerbus/bus"
	"github.com/aperturerobotics/controllerbus/directive"
	"github.com/aperturerobotics/util/broadcast"
	"github.com/s4wave/spacewave/core/transport"
	"github.com/s4wave/spacewave/db/world"
	"github.com/s4wave/spacewave/net/peer"
	s4wave_world "github.com/s4wave/spacewave/sdk/world"
)

// WatchDeviceAvailability joins one Device object watch with the observing
// Session's authenticated link watch. It emits an initial projection, then only
// changes, until ctx ends or emit fails. Link events do not read or write World
// state: each checks one indexed peer entry per active transport controller.
// Missing observer identity or an invalid Device peer reports Unknown.
func WatchDeviceAvailability(ctx context.Context, b bus.Bus, sessionPeerID peer.ID, ws world.WorldState, objectKey string, emit func(*DeviceAvailability) error) error {
	// Retain transport discovery across Session shutdown and restart.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var bcast broadcast.Broadcast
	var currentTransport *transport.SessionTransport
	var currentDevice *Device
	if sessionPeerID != "" {
		_, ref, err := bus.ExecOneOffWatchCb(ctx, func(value directive.TypedAttachedValue[*transport.SessionTransport]) bool {
			bcast.HoldLock(func(broadcast func(), _ func() <-chan struct{}) {
				currentTransport = nil
				if value != nil {
					currentTransport = value.GetValue()
				}
				broadcast()
			})
			return true
		}, b, transport.NewLookupSessionTransport(sessionPeerID))
		if err != nil {
			return err
		}
		defer ref.Release()
	}

	// Keep World reads on object revisions, independently of link notifications.
	done := make(chan struct{})
	var readErr error
	go func() {
		defer close(done)
		readErr = s4wave_world.WatchWorldObject(ctx, ws, objectKey, readDeviceObject, func(device *Device, changed bool) error {
			if changed {
				bcast.HoldLock(func(broadcast func(), _ func() <-chan struct{}) {
					currentDevice = device
					broadcast()
				})
			}
			return nil
		})
	}()
	defer func() { cancel(); <-done }()

	// Join snapshots with their wait channels so neither source can lose a wakeup.
	var previous *DeviceAvailability
	for {
		// Read only cached Device state when a transport event wakes the projection.
		var device *Device
		var st *transport.SessionTransport
		var changed <-chan struct{}
		bcast.HoldLock(func(_ func(), getWaitCh func() <-chan struct{}) {
			device, st, changed = currentDevice, currentTransport, getWaitCh()
		})
		wait := []<-chan struct{}{changed, done}

		// Keep unavailable identities distinct from known peers without a live link.
		current := &DeviceAvailability{Device: device}
		remote, peerErr := peer.IDB58Decode(device.GetPeerId())
		if peerErr == nil && sessionPeerID != "" {
			current.State = DeviceAvailabilityState_DEVICE_AVAILABILITY_STATE_OFFLINE
			if st != nil {
				linked, linkChanged := st.GetLinkedPeerIDsSnapshotWithWait([]peer.ID{remote})
				wait = append(wait, linkChanged...)
				if _, ok := linked[remote]; ok {
					current.State = DeviceAvailabilityState_DEVICE_AVAILABILITY_STATE_ONLINE
				}
			}
		}

		// Emit changes outside the source locks and protect cached records from mutation.
		if device != nil && !current.EqualVT(previous) {
			if err := emit(current.CloneVT()); err != nil {
				return err
			}
			previous = current
		}

		// End on World failure as well as cancellation or the next watched change.
		select {
		case <-done:
			return readErr
		default:
		}
		if err := broadcast.WaitAny(ctx, wait...); err != nil {
			return err
		}
	}
}
