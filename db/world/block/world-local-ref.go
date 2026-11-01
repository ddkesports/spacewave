package world_block

import "github.com/s4wave/spacewave/db/bucket"

// localObjectRef keeps same-bucket bodies in the World DAG, including empty
// bodies whose root pointer may disappear across an RPC.
func (t *WorldState) localObjectRef(ref *bucket.ObjectRef) *bucket.ObjectRef {
	if t.localBucketID == "" || ref == nil || ref.GetBucketId() != t.localBucketID {
		return ref
	}
	local := ref.Clone()
	local.BucketId = ""
	return local
}

// externalObjectRef restores the current bucket on a local World DAG edge.
func (t *WorldState) externalObjectRef(ref *bucket.ObjectRef) *bucket.ObjectRef {
	if t.localBucketID == "" || ref == nil || ref.GetBucketId() != "" {
		return ref
	}
	external := ref.Clone()
	external.BucketId = t.localBucketID
	return external
}
