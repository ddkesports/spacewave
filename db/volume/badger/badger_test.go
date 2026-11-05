//go:build !js && !wasip1

package volume_badger_test

import (
	"context"
	"testing"

	"github.com/aperturerobotics/controllerbus/controller/loader"
	"github.com/aperturerobotics/controllerbus/controller/resolver"
	"github.com/s4wave/spacewave/db/core"
	"github.com/s4wave/spacewave/db/volume"
	volume_badger "github.com/s4wave/spacewave/db/volume/badger"
	volume_test "github.com/s4wave/spacewave/db/volume/test"
	"github.com/sirupsen/logrus"
)

// TestBadgerVolume runs the volume contract through the controller, with the
// options a production volume uses.
func TestBadgerVolume(t *testing.T) {
	// Register the Badger factory on the test bus.
	ctx := context.Background()
	le := logrus.NewEntry(logrus.New())
	b, sr, err := core.NewCoreBus(ctx, le)
	if err != nil {
		t.Fatal(err)
	}
	sr.AddFactory(volume_badger.NewFactory(b))

	// Load the Badger volume controller for a temporary directory.
	volCtrl, _, diRef, err := loader.WaitExecControllerRunningTyped[volume.Controller](
		ctx,
		b,
		resolver.NewLoadControllerWithConfig(&volume_badger.Config{Dir: t.TempDir()}),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer diRef.Release()

	// Obtain the mounted volume.
	vol, err := volCtrl.GetVolume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the storage contract.
	if err := volume_test.CheckVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}

	// Verify the key count, which is exact. Badger refreshes its byte sizes
	// once a minute, so TotalBytes lags.
	stats, err := vol.GetStorageStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.GetBlockCount() == 0 {
		t.Fatal("expected a nonzero block count after writing data")
	}
}
