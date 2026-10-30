//go:build !tinygo && !goscript

package objecttypes

import (
	"context"

	space_world "github.com/s4wave/spacewave/core/space/world"
	"github.com/s4wave/spacewave/sdk/world/objecttype"
	wizard_resource "github.com/s4wave/spacewave/sdk/world/wizard/resource"
)

// LookupObjectType looks up an object type by ID, resolving the legacy
// SpaceSettings ID. It returns nil only when the wizard registry has no
// matching type.
func LookupObjectType(ctx context.Context, typeID string) (objecttype.ObjectType, error) {
	if typeID == space_world.LegacySpaceSettingsTypeID {
		typeID = space_world.SpaceSettingsTypeID
	}
	if objectType := compiledObjectTypes[typeID]; objectType != nil {
		return objectType, nil
	}
	return wizard_resource.LookupWizardObjectType(ctx, typeID)
}
