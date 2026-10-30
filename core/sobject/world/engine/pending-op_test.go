package sobject_world_engine

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/aperturerobotics/controllerbus/controller"
	"github.com/aperturerobotics/controllerbus/directive"
	"github.com/s4wave/spacewave/core/sobject"
	"github.com/s4wave/spacewave/db/world"
	world_block_tx "github.com/s4wave/spacewave/db/world/block/tx"
	world_mock "github.com/s4wave/spacewave/db/world/mock"
)

// opPluginRecorder reports the plugins each LoadOpPlugins directive asks for,
// standing in for a Space resource's plugin loader.
type opPluginRecorder struct {
	// loads receives the requested plugins.
	loads chan<- []string
}

// GetControllerInfo returns information about the controller.
func (r *opPluginRecorder) GetControllerInfo() *controller.Info {
	return controller.NewInfo("test/op-plugin-recorder", controller.MustParseVersion("0.0.1"), "records LoadOpPlugins")
}

// Execute returns immediately.
func (r *opPluginRecorder) Execute(context.Context) error {
	return nil
}

// HandleDirective reports the plugins a LoadOpPlugins asks for.
func (r *opPluginRecorder) HandleDirective(_ context.Context, di directive.Instance) ([]directive.Resolver, error) {
	if dir, ok := di.GetDirective().(LoadOpPlugins); ok {
		select {
		case r.loads <- dir.LoadOpPluginsPluginIDs():
		default:
		}
	}
	return nil, nil
}

// Close releases nothing.
func (r *opPluginRecorder) Close() error {
	return nil
}

// TestReplayWaitsForDeclaredOpHandler replays one log on two replicas. The
// World declares that a plugin handles the edit's operation type. Replica A
// resolves the type from the start; replica B's handler registers only after
// the edit arrived. B must stop at the edit, ask for the declaring plugin,
// wait for its handler, and then reach A's World with A's outcomes. An
// operation whose type no plugin declares is rejected on both replicas.
func TestReplayWaitsForDeclaredOpHandler(t *testing.T) {
	// Build the Space with two writers.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	privA, pidA := newReplayTestKey(t)
	_, pidB := newReplayTestKey(t)
	space := newReplayTestSpace(t, pidA, pidB)

	// Declare the mock object operation as a plugin's, with A's handler built
	// in and B's registered later on the bus.
	declared := func(_ context.Context, _ world.WorldState, opTypeID string) ([]string, error) {
		if opTypeID == world_mock.MockObjectOpTypeID {
			return []string{"test-plugin"}, nil
		}
		return nil, nil
	}
	space.c.SetOpTypes(OpTypes{Lookup: world_mock.LookupMockOp, Declared: declared})
	late := &Controller{le: space.c.le, bus: space.c.bus, conf: space.c.conf, sfs: space.c.sfs}
	late.SetOpTypes(OpTypes{Declared: declared})

	// Create an object, edit it with the declared operation, edit it with an
	// undeclared one, and create another object.
	txOp := func(name string, nonce uint64, prev *sobject.SOOperation, opTypeID string) *sobject.SOOperation {
		t.Helper()
		tx, err := world_block_tx.NewTxApplyObjectOp(opTypeID, world_mock.NewMockObjectOp(name), "object", pidA)
		if err != nil {
			t.Fatal(err.Error())
		}
		return space.signTx(name, privA, tx, &sobject.SOOperationLink{Nonce: nonce, PrevOpHash: prev.Hash()})
	}
	create := space.sign("create", privA, "object", &sobject.SOOperationLink{Nonce: 1})
	edit := txOp("edit", 2, create, world_mock.MockObjectOpTypeID)
	undeclared := txOp("undeclared", 3, edit, "test/undeclared")
	after := space.sign("after", privA, "object-after", &sobject.SOOperationLink{Nonce: 4, PrevOpHash: undeclared.Hash()})
	ops := []*sobject.SOOperation{create, edit, undeclared, after}

	// Replica A applies the edit and rejects only the undeclared operation.
	replicaA := space.member(pidA, false)
	replicaA.replayer.lookupOp = space.c.buildLookupWorldOp(space.c.le)
	want := replicaA.deliver(ops...)
	for i, outcome := range want.outcomes {
		applied := outcome == space.names[string(ops[i].Hash())]+": applied"
		if applied != (ops[i] != undeclared) {
			t.Fatalf("replica A outcomes %q; want only the undeclared operation rejected", want.outcomes)
		}
	}

	// Replica B stops at the edit with the World after the creation, waiting
	// for its declared plugin.
	replicaB := space.member(pidB, false)
	replicaB.replayer.c, replicaB.replayer.lookupOp = late, late.buildLookupWorldOp(late.le)
	for _, op := range ops {
		if _, err := replicaB.set.Add(op); err != nil {
			t.Fatal(err.Error())
		}
	}
	replicaB.ops = ops
	stopped, _, err := replicaB.replayer.replay(ctx, replicaB.snap, replicaB.set, nil)
	gap := gapOf(err, nil)
	if !isReplayGap(err) || stopped == nil || gap.pending == nil || gap.pending.opTypeID != world_mock.MockObjectOpTypeID {
		t.Fatalf("replica B before its handler = %v, gap %+v; want a stop at the declared operation", err, gap)
	}

	// B asks for the declaring plugin while it waits.
	loads := make(chan []string, 1)
	relLoader, err := space.c.bus.AddController(ctx, &opPluginRecorder{loads: loads}, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer relLoader()
	waited := make(chan error, 1)
	go func() { waited <- late.waitGap(ctx, replicaB.replayer.so, gap) }()
	if ids := <-loads; !slices.Equal(ids, []string{"test-plugin"}) {
		t.Fatalf("replica B asked for plugins %q; want test-plugin", ids)
	}

	// The plugin registers its handler, which ends B's wait.
	rel, err := space.c.bus.AddController(ctx, world.NewLookupOpController("test-plugin", "", world_mock.LookupMockOp), nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer rel()
	if err := <-waited; err != nil {
		t.Fatalf("replica B waiting for its handler: %v", err)
	}

	// Replica B resumes at the edit and reaches A's World and outcomes.
	resumed := replicaB.deliver()
	if !slices.Equal(resumed.outcomes, want.outcomes) {
		t.Fatalf("replica B outcomes %q; want %q", resumed.outcomes, want.outcomes)
	}
	if !resumed.state.EqualVT(want.state) {
		t.Fatal("replica B World differs from replica A's")
	}
}
