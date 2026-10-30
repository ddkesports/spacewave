package provider_local

import (
	"testing"
	"time"

	"github.com/s4wave/spacewave/net/hash"
)

// TestReadSetKeepsRecentReads checks that eviction keeps a read block across
// passes until two generations of readKeepAge have turned.
func TestReadSetKeepsRecentReads(t *testing.T) {
	// Record a read.
	var r readSet
	r.enabled.Store(true)
	read, other := hash.NewHash(hash.HashType_HashType_SHA256, []byte("read")), hash.NewHash(hash.HashType_HashType_SHA256, []byte("other"))
	r.add(read)

	// Passes in quick succession keep it.
	for range 3 {
		if keep := r.take(); !keep(read) || keep(other) {
			t.Fatal("pass did not keep exactly the read block")
		}
	}

	// One turn keeps it in the previous generation, the second drops it.
	r.turned = time.Now().Add(-readKeepAge)
	if !r.take()(read) {
		t.Fatal("first turn dropped the read block")
	}
	r.turned = time.Now().Add(-readKeepAge)
	if r.take()(read) {
		t.Fatal("second turn kept the read block")
	}
}
