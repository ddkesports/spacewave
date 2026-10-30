package v86_wazero

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	cdn_sharedobject "github.com/s4wave/spacewave/core/cdn/sharedobject"
)

// TestRunImage runs a program in the published fixture image through
// RunImage: the program sees its arguments and environment, its output reaches
// the console, and its exit code returns once the guest halts.
func TestRunImage(t *testing.T) {
	// Require the boot fixture, which reads the image from the CDN.
	if !runV86WazeroBootTests() {
		t.Skip("set RUN_V86_WAZERO_BOOT=true to boot the published V86Image")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	// Read the image from its published World.
	opts := OptionsFromEnv().withDefaults()
	ws, err := cdn_sharedobject.MountWorld(ctx, opts.Le, nil, opts.CdnBaseURL, opts.CdnSpaceID)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Release()

	// Run a program that prints its environment and exits 3.
	var console bytes.Buffer
	code, err := RunImage(ctx, opts.Le, nil, ws, opts.ImageKey, &RunConfig{
		Path:    "/bin/sh",
		Args:    []string{"-c", `echo "hello $GREETING"; exit 3`},
		Env:     []string{"GREETING=it's me"},
		Console: &console,
	})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, console.String())
	}
	if code != 3 || !strings.Contains(console.String(), "hello it's me") {
		t.Fatalf("exit code %d, console:\n%s", code, console.String())
	}
}
