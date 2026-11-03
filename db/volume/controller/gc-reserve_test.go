package volume_controller

import (
	"testing"

	"github.com/s4wave/spacewave/db/volume"
)

// reserveHealth is a volume health stuck in the disk reserve.
type reserveHealth struct{}

// GetHealthSnapshotWithWait reports the reserve write state.
func (reserveHealth) GetHealthSnapshotWithWait() (*volume.VolumeHealth, <-chan struct{}) {
	return &volume.VolumeHealth{WriteState: volume.VolumeWriteState_VOLUME_WRITE_STATE_RESERVE}, nil
}

// TestReserveWatchHonoursReservePassOff checks that a volume entering its disk
// reserve pulls a block pass in by default, and starts none while reserve
// passes are off.
func TestReserveWatchHonoursReservePassOff(t *testing.T) {
	defer func(off bool) { reservePassOff = off }(reservePassOff)
	for _, off := range []bool{false, true} {
		reservePassOff = off
		if entered, _ := newReserveWatch(reserveHealth{}).entered(); entered == off {
			t.Fatalf("reserve passes off=%v: entered reserve reported %v", off, entered)
		}
	}
}
