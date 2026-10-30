//go:build !js

package wasivm_plugin

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/s4wave/spacewave/db/world"
	db_world_testbed "github.com/s4wave/spacewave/db/world/testbed"
	s4wave_vm "github.com/s4wave/spacewave/sdk/vm"
)

// TestImageRootfs checks that a VM with no rootfs edge of its own boots the
// root filesystem of its linked image, and that Run runs a program from the
// image with its arguments and environment and returns its exit code.
func TestImageRootfs(t *testing.T) {
	// Start a World testbed.
	ctx := t.Context()
	wtb, err := db_world_testbed.Default(ctx, db_world_testbed.WithWorldVerbose(false))
	if err != nil {
		t.Fatal(err)
	}
	defer wtb.Release()
	ws := world.NewEngineWorldState(wtb.Engine, true)

	// Create a rootfs holding the program.
	const rootfsKey, imageKey, vmKey = "fs/rootfs", "vm-image/test", "vm/wasivm/test"
	initWasm, err := os.ReadFile("testdata/wasivm-args.wasm")
	if err != nil {
		t.Fatal(err)
	}
	writeRootfsFile(t, ws, rootfsKey, "init", initWasm)

	// Create an image on the rootfs and a VM on the image.
	createImage := s4wave_vm.NewCreateWasiVmImageOp(imageKey, &s4wave_vm.WasiVmImage{Name: "test"}, time.Unix(1, 0))
	if _, _, err := ws.ApplyWorldOp(ctx, createImage, ""); err != nil {
		t.Fatal(err)
	}
	edge := world.NewGraphQuadWithKeys(imageKey, string(s4wave_vm.PredWasiVmImageRootfs), rootfsKey, "")
	if err := ws.SetGraphQuad(ctx, edge); err != nil {
		t.Fatal(err)
	}
	createVm := s4wave_vm.NewCreateWasiVmOp(vmKey, "test", "", imageKey, time.Unix(1, 0))
	if _, _, err := ws.ApplyWorldOp(ctx, createVm, ""); err != nil {
		t.Fatal(err)
	}

	// The VM resolves the image's rootfs in the same World.
	src, err := resolveRootfs(ctx, wtb.Logger, nil, ws, vmKey)
	if err != nil {
		t.Fatal(err)
	}
	defer src.release()
	if src.key != rootfsKey || src.ws != ws {
		t.Fatalf("rootfs = %q, want %q in the VM's World", src.key, rootfsKey)
	}

	// Run the image's program, which prints its arguments and environment.
	var stdout bytes.Buffer
	code, err := Run(ctx, wtb.Logger, nil, ws, imageKey, &RunConfig{
		Path:   "/init",
		Args:   []string{"hello"},
		Env:    map[string]string{"GREETING": "hi"},
		Stdout: &stdout,
	})
	if err != nil {
		t.Fatal(err)
	}
	out := stdout.String()
	if code != 3 || !strings.Contains(out, "/init\x00hello\x00") || !strings.Contains(out, "GREETING=hi\x00") || !strings.Contains(out, "PATH=") {
		t.Fatalf("exit code %d, output %q", code, out)
	}
}
