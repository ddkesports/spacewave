//go:build darwin || linux || windows

package s4db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
)

// TestReadOnly checks that a read-only handle reads the committed keys,
// refuses writes and compaction, leaves the writer lock free, never creates
// a file, and does not change the file's length.
func TestReadOnly(t *testing.T) {
	// Choose a file in a temporary directory.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ro.s4wave")

	// A missing file is not opened.
	if _, err := Open(path, Options{ReadOnly: true}); err == nil {
		t.Fatal("read-only open of a missing file succeeded")
	}

	// A missing file is not created.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("read-only open created the file: %v", err)
	}

	// Write keys and close.
	db := openTest(t, path, Options{})
	if err := writeKeys(ctx, db, "ro"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Open a read-only handle.
	ro := openTest(t, path, Options{ReadOnly: true})

	// Read the keys back.
	m := make(model)
	for i := range childKeys {
		m[string(processKey("ro", i))] = processValue(i)
	}
	m.check(t, ro)

	// Refuse writes and compaction.
	if _, err := ro.NewTransaction(ctx, true); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("write transaction: %v", err)
	}
	if err := ro.Compact(ctx); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("compact: %v", err)
	}

	// Leave the writer lock free for the real writer.
	if got := probeWriterLock(t, path); got != "free" {
		t.Fatalf("read-only handle: probe found the writer lock %s", got)
	}

	// Close without changing a byte of the file.
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("a read-only open, read and close changed the file")
	}

	// A writer still opens the file afterwards.
	db = openTest(t, path, Options{})
	defer db.Close()
	m.check(t, db)
}
