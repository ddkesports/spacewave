package blocktype

import "github.com/pkg/errors"

// MarshalJSON decodes data as a block of type bt and renders it as JSON.
func MarshalJSON(bt BlockType, data []byte) ([]byte, error) {
	// Decode the block, then render it through its generated JSON form.
	blk := bt.Constructor()
	if err := blk.UnmarshalVT(data); err != nil {
		return nil, err
	}
	m, ok := blk.(interface{ MarshalJSON() ([]byte, error) })
	if !ok {
		return nil, errors.Errorf("block type %s has no JSON form", bt.GetBlockTypeID())
	}
	return m.MarshalJSON()
}

// RenderJSON renders each body as a block of type bt in JSON, preserving
// order. An empty string marks a body that does not decode as bt.
func RenderJSON(bt BlockType, bodies [][]byte) []string {
	out := make([]string, len(bodies))
	for i, body := range bodies {
		if js, err := MarshalJSON(bt, body); err == nil {
			out[i] = string(js)
		}
	}
	return out
}
