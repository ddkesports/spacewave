package volume_controller

import (
	"os"

	"github.com/s4wave/spacewave/db/volume"
)

// ReservePassEnv names the environment variable that stops a volume entering
// its disk reserve from starting a block pass when set to "off". A process
// that keeps block GC off by a long interval sets it, so a full disk refuses
// writes instead of running a pass over a reference graph it never completed;
// unset keeps the pass, which frees the space a collected volume waits on.
const ReservePassEnv = "SPACEWAVE_GC_RESERVE_PASS" // #nosec G101 -- an environment variable name, not a credential.

// reservePassOff is read once at startup from ReservePassEnv.
var reservePassOff = os.Getenv(ReservePassEnv) == "off"

// newReserveWatch returns the reserve watch of a volume's health, which
// never reports entering when reserve passes are off.
func newReserveWatch(hw volume.HealthWatcher) *reserveWatch {
	if reservePassOff {
		return &reserveWatch{}
	}
	return &reserveWatch{hw: hw}
}

// reserveWatch tells the GC loop when its volume enters the disk reserve, so
// a volume that refuses writes collects at once instead of at the next
// scheduled block pass.
type reserveWatch struct {
	// hw is the volume's health, nil when it publishes none.
	hw volume.HealthWatcher
	// in records the write state of the last snapshot.
	in bool
}

// entered snapshots the health and reports whether the volume entered the
// reserve since the last call, with a channel that closes when the health
// next changes. A volume without health never enters and returns a nil
// channel, which a select never receives from.
func (w *reserveWatch) entered() (bool, <-chan struct{}) {
	// Snapshot the write state.
	if w.hw == nil {
		return false, nil
	}
	h, waitCh := w.hw.GetHealthSnapshotWithWait()

	// Report a move into the reserve once.
	in := h.GetWriteState() == volume.VolumeWriteState_VOLUME_WRITE_STATE_RESERVE
	entered := in && !w.in
	w.in = in
	return entered, waitCh
}
