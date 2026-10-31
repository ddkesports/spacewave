package packmove

import (
	"github.com/s4wave/spacewave/db/kvtx"
	"github.com/s4wave/spacewave/db/packfile/writer"
	store_kvkey "github.com/s4wave/spacewave/db/store/kvkey"
)

// DefaultPackBytes is the block data one pack holds when PackConfig.PackBytes
// is unset: half the packfile limit, so a pack's encoding of the blocks and
// their references stays well inside it.
const DefaultPackBytes = int(writer.DefaultMaxPackBytes / 2)

// PackConfig configures Pack.
type PackConfig struct {
	// Source is the volume store whose blocks are packed. Pack only reads it,
	// and may share the file with the process that writes it.
	Source kvtx.Store
	// Keys names the key layout of Source.
	Keys *store_kvkey.KVKey
	// WorldStoreID is the bucket ID of the World's block store. Pack leaves
	// alone the blocks the reference graph reaches from any other root.
	WorldStoreID string
	// Packer receives the packs.
	Packer BlockPacker
	// LogPath is the pack log, created when missing. A log from an earlier
	// run resumes that run.
	LogPath string
	// PackBytes is the block data one pack holds; zero uses DefaultPackBytes.
	PackBytes int
	// BytesPerSecond limits the block data uploaded per second; zero or less
	// does not limit.
	BytesPerSecond int64
	// SkipRest leaves out the measure of the keys outside the block range.
	SkipRest bool
	// Progress receives the totals after each committed pack and
	// each measured chunk; nil reports nothing. Calls do not overlap.
	Progress func(PackProgress)
}
