package sobject_replica_controller

import (
	"context"

	"github.com/aperturerobotics/controllerbus/directive"
	"github.com/aperturerobotics/util/ccontainer"
	"github.com/pkg/errors"
	provider_local "github.com/s4wave/spacewave/core/provider/local"
	"github.com/s4wave/spacewave/core/sobject"
	sobject_world_engine "github.com/s4wave/spacewave/core/sobject/world/engine"
	"github.com/s4wave/spacewave/core/space"
	"github.com/s4wave/spacewave/db/world"
)

// mount is the replicated Space mounted on this Device while it participates.
type mount struct {
	// so is the mounted SharedObject.
	so *provider_local.SharedObject
	// state watches the SharedObject state.
	state ccontainer.Watchable[sobject.SharedObjectStateSnapshot]
	// engine is the Space's World engine.
	engine world.Engine
	// release releases the state and the Space body.
	release func()
}

// openMount mounts the Space body of ref and its SharedObject state.
func openMount(ctx context.Context, r *reconciler, ref *sobject.SharedObjectRef) (*mount, error) {
	// Mount the Space body, which runs its World engine.
	body, bodyRef, err := space.ExMountSpaceSoBody(ctx, r.c.b, ref, false, nil)
	if err != nil {
		return nil, err
	}
	so, ok := body.GetSharedObject().(*provider_local.SharedObject)
	if !ok {
		bodyRef.Release()
		return nil, errors.New("the replicated SharedObject is not held by the local provider")
	}

	// Watch the SharedObject state.
	state, releaseState, err := so.AccessSharedObjectState(ctx, nil)
	if err != nil {
		bodyRef.Release()
		return nil, err
	}
	return &mount{
		so:      so,
		state:   state,
		engine:  body.GetSharedObjectBody().GetWorldEngine(),
		release: releaseAll(releaseState, bodyRef),
	}, nil
}

// accessBackfill calls cb with the backfill engine serving the Space's World.
func (m *mount) accessBackfill(ctx context.Context, cb func(be sobject_world_engine.BackfillEngine) error) error {
	return world.AccessEngine(ctx, m.engine, func(_ context.Context, eng world.Engine) error {
		be, ok := eng.(sobject_world_engine.BackfillEngine)
		if !ok {
			return errors.New("the replicated World cannot be backfilled")
		}
		return cb(be)
	})
}

// releaseAll returns a release of the state, then the directive reference.
func releaseAll(releaseState func(), ref directive.Reference) func() {
	return func() {
		releaseState()
		ref.Release()
	}
}
