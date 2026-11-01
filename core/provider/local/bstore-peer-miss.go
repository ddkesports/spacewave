package provider_local

import (
	"context"
	"sync"

	"github.com/s4wave/spacewave/db/block"
	"github.com/sirupsen/logrus"
)

// peerMissLog is the peer source of a placed store's reads. A read reaches it
// only after the local store, the backend and the read fallback all missed, so
// it warns once per block with the block's ref before asking peers.
type peerMissLog struct {
	// StoreOps is the peer store.
	block.StoreOps
	// le logs the misses.
	le *logrus.Entry
	// seen holds the refs already logged, keyed by their marshaled string.
	seen *sync.Map
}

// newPeerMissSource wraps peers so each block that reaches it is logged once.
func newPeerMissSource(le *logrus.Entry, peers func() block.StoreOps) func() block.StoreOps {
	seen := &sync.Map{}
	return func() block.StoreOps {
		store := peers()
		if store == nil {
			return nil
		}
		return &peerMissLog{StoreOps: store, le: le, seen: seen}
	}
}

// GetBlock logs the miss and reads the block from peers.
func (p *peerMissLog) GetBlock(ctx context.Context, ref *block.BlockRef) ([]byte, bool, error) {
	p.logMiss(ref)
	return p.StoreOps.GetBlock(ctx, ref)
}

// GetStoredBlock logs the miss and reads the block and its refs from peers.
func (p *peerMissLog) GetStoredBlock(ctx context.Context, ref *block.BlockRef) (*block.StoredBlock, error) {
	p.logMiss(ref)
	return p.StoreOps.GetStoredBlock(ctx, ref)
}

// logMiss warns the first time ref reaches the peers.
func (p *peerMissLog) logMiss(ref *block.BlockRef) {
	key := ref.MarshalString()
	if _, loaded := p.seen.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	p.le.WithField("ref", key).Warn("block missing from the local store, the backend and the fallback; asking peers")
}
