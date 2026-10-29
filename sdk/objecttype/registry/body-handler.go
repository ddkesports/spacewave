package s4wave_objecttype_registry

import (
	"context"

	"github.com/s4wave/spacewave/db/blocktype"
)

// BodyHandler serves ObjectTypeBodyService from a plugin's block types.
type BodyHandler struct {
	// lookup resolves an ObjectType ID to the block type of its root body.
	lookup func(ctx context.Context, typeID string) (blocktype.BlockType, error)
}

// NewBodyHandler builds a BodyHandler over lookup, such as a plugin World
// schema's LookupBlockType.
func NewBodyHandler(lookup func(ctx context.Context, typeID string) (blocktype.BlockType, error)) *BodyHandler {
	return &BodyHandler{lookup: lookup}
}

// MarshalBodiesJSON renders root block bodies of one ObjectType as JSON. A
// type lookup does not know leaves every body without JSON.
func (h *BodyHandler) MarshalBodiesJSON(ctx context.Context, req *MarshalBodiesJSONRequest) (*MarshalBodiesJSONResponse, error) {
	bt, err := h.lookup(ctx, req.GetTypeId())
	if err != nil {
		return nil, err
	}
	if bt == nil {
		return &MarshalBodiesJSONResponse{BodiesJson: make([]string, len(req.GetBodies()))}, nil
	}
	return &MarshalBodiesJSONResponse{BodiesJson: blocktype.RenderJSON(bt, req.GetBodies())}, nil
}

// _ is a type assertion
var _ SRPCObjectTypeBodyServiceServer = (*BodyHandler)(nil)
