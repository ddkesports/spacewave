package block

// PutBatchEntry describes one durable block write in a lower-layer batch.
type PutBatchEntry struct {
	// Ref is the expected content-addressed block reference.
	Ref *BlockRef
	// Data is the encoded block payload.
	Data []byte
	// Refs are outgoing block references recorded with this write.
	Refs []*BlockRef
	// RefsKnown declares Refs complete, as PutOpts.RefsKnown does.
	RefsKnown bool
	// Tombstone marks the block ref as deleted.
	Tombstone bool
	// Counted reports that the writer found the block stored before writing
	// it and counted the holder it adds, so a store that counts shared blocks
	// adds no count when the put finds it. A layer that drops the flag
	// over-counts, which leaks and never frees.
	Counted bool
}
