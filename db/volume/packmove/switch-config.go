package packmove

import (
	"github.com/s4wave/spacewave/db/kvtx"
	store_kvkey "github.com/s4wave/spacewave/db/store/kvkey"
	volume_kvtx "github.com/s4wave/spacewave/db/volume/common/kvtx"
)

// SwitchConfig configures Switch.
type SwitchConfig struct {
	// Source is the volume store to copy. No other process may write it.
	Source kvtx.Store
	// Dest is the empty store that receives the copy.
	Dest kvtx.Store
	// Keys names the key layout of both stores.
	Keys *store_kvkey.KVKey
	// LogPath is the pack log Pack wrote.
	LogPath string
	// WorldStoreID is the bucket ID of the World's block store, as Pack was
	// given it.
	WorldStoreID string
	// WritebackPrefix is the key prefix of the World store's write-back object
	// store: the block store's GetObjectStorePrefixByID of its write-back
	// object store ID. The copy leaves out every key under it, which holds the
	// source's upload and eviction records, and writes fresh upload markers
	// under it.
	WritebackPrefix []byte
	// Progress receives the copy's counts after each commit; nil reports nothing.
	Progress func(volume_kvtx.CopyProgress)
}
