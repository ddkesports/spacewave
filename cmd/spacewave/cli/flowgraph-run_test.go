//go:build !js

package spacewave_cli

import (
	"context"
	"testing"

	"github.com/aperturerobotics/cli"
	"github.com/s4wave/spacewave/db/world"
	world_testbed "github.com/s4wave/spacewave/db/world/testbed"
	forge_cluster "github.com/s4wave/spacewave/forge/cluster"
	forge_world "github.com/s4wave/spacewave/forge/world"
	"github.com/s4wave/spacewave/net/peer"
	flowgraph "github.com/s4wave/spacewave/sdk/flowgraph"
	flowgraph_run "github.com/s4wave/spacewave/sdk/flowgraph/run"
)

// TestFlowgraphStandingRun starts a standing run from CLI flags and finds it again.
func TestFlowgraphStandingRun(t *testing.T) {
	// Start a World with Forge operations and an empty graph assigned to a Cluster.
	ctx := t.Context()
	tb := world_testbed.MustDefault(t, ctx)
	sender, _, _, err := peer.NewPeerWithGenerateED25519()
	if err != nil {
		t.Fatal(err)
	}
	pid := sender.GetPeerID()
	opc := world.NewLookupOpController("forge-ops", tb.EngineID, world.NewSchema(forge_world.Types).LookupOp)
	release, err := tb.Bus.AddController(ctx, opc, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	err = world.ExecTransaction(ctx, tb.Engine, true, func(ctx context.Context, tx world.WorldState) error {
		if _, err := (&flowgraph.CreateFlowgraphOp{ObjectKey: "flowgraph/test", Name: "test"}).ApplyWorldOp(ctx, nil, tx, pid); err != nil {
			return err
		}
		_, _, err := forge_cluster.CreateCluster(ctx, tx, "cluster/test", "test", pid, pid)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Parse the real command flags and apply its durable transition on the World.
	args := &flowgraphRunArgs{action: "start"}
	app := cli.NewApp()
	app.Flags = args.BuildFlags()
	app.Action = func(c *cli.Context) error {
		return world.ExecTransaction(c.Context, tb.Engine, true, func(ctx context.Context, tx world.WorldState) error {
			return args.apply(ctx, tx, pid, flowgraph_run.StandingRunKey(args.key))
		})
	}
	command := []string{"spacewave", "--standing", "--flowgraph", "flowgraph/test", "--cluster", "cluster/test"}
	if err := app.RunContext(ctx, command); err != nil {
		t.Fatal(err)
	}
	ws := world.NewEngineWorldState(tb.Engine, false)
	first, revision, err := flowgraph.ReadFlowgraphRun(ctx, ws, flowgraph_run.StandingRunKey(args.key))
	if err != nil || !first.GetStanding() {
		t.Fatalf("first run = %v, %v", first, err)
	}

	// A second invocation finds the same run without rewriting its Job or revision.
	if err := app.RunContext(ctx, command); err != nil {
		t.Fatal(err)
	}
	second, nextRevision, err := flowgraph.ReadFlowgraphRun(ctx, ws, flowgraph_run.StandingRunKey(args.key))
	if err != nil || !second.EqualVT(first) || nextRevision != revision {
		t.Fatalf("second run = %v, %v", second, err)
	}
}
