package provider_transfer

import "context"

// TransferOptions carries the optional collaborators of a Transfer. The zero
// value is valid: no cleanup, no checkpointing, and no space-ID filter.
type TransferOptions struct {
	// Cleanup deletes the source volume after a successful transfer.
	Cleanup CleanupSource
	// Checkpoint persists resumable transfer progress.
	Checkpoint CheckpointStore
	// FilterSpaceIDs restricts the transfer to the listed Space IDs. With
	// Cleanup set, Merge and Migrate delete the whole source volume, so they
	// refuse a filter that leaves out a Space.
	FilterSpaceIDs []string
	// CopyIdentity copies the source's peer key to the target. Migrate signs
	// the target Spaces with that key, so Execute runs it after every Space
	// reaches the target and before Cleanup deletes the source.
	CopyIdentity func(ctx context.Context) error
}
