package resource_flowgraph_registry

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/aperturerobotics/controllerbus/bus"
	"github.com/aperturerobotics/controllerbus/config"
	"github.com/pkg/errors"
	s4wave_flowgraph "github.com/s4wave/spacewave/sdk/flowgraph"
	flowgraph_registry "github.com/s4wave/spacewave/sdk/flowgraph/registry"
)

// compileTimeout bounds one plugin Compile call, because the Device reconciles
// every node on one loop and an unresponsive plugin would stall all of them.
const compileTimeout = 30 * time.Second

// nodeTypeProxy compiles placements through a plugin's retained Resource capability.
type nodeTypeProxy struct {
	// b resolves compiled core controller config constructors on the daemon bus.
	b bus.Bus
	// reg is the registration whose plugin compiles and runs the node.
	reg *nodeTypeRegistration
	// client invokes the registering plugin's compiler.
	client flowgraph_registry.SRPCNodeTypeResourceServiceClient
	// configIDs is the set of declared controller config IDs.
	configIDs map[string]struct{}
	// pluginConfigIDs is the set of config IDs that run inside the plugin.
	pluginConfigIDs map[string]struct{}
}

// newNodeTypeProxy constructs a compiler bound to one registration lifetime.
func newNodeTypeProxy(b bus.Bus, reg *nodeTypeRegistration) *nodeTypeProxy {
	return &nodeTypeProxy{
		b:               b,
		reg:             reg,
		client:          flowgraph_registry.NewSRPCNodeTypeResourceServiceClient(reg.client),
		configIDs:       idSet(reg.metadata.GetConfigIds()),
		pluginConfigIDs: idSet(reg.metadata.GetPluginConfigIds()),
	}
}

// GetDisplayName returns the plugin's display name.
func (p *nodeTypeProxy) GetDisplayName() string {
	return p.reg.metadata.GetDisplayName()
}

// GetPorts returns an independent copy of the declared endpoints.
func (p *nodeTypeProxy) GetPorts() []*s4wave_flowgraph.FlowgraphPort {
	return p.reg.metadata.CloneVT().GetPorts()
}

// GetConfigIDs returns an independent copy of the allowed controller config IDs.
func (p *nodeTypeProxy) GetConfigIDs() []string {
	return slices.Clone(p.reg.metadata.GetConfigIds())
}

// Compile invokes the plugin and decodes only its declared controller config IDs.
func (p *nodeTypeProxy) Compile(ctx context.Context, node *s4wave_flowgraph.PlacedFlowgraphNode) (map[string]config.Config, error) {
	// Cancel the call when the Device or the registration ends, or the plugin stalls.
	ctx, cancel := context.WithTimeout(ctx, compileTimeout)
	defer cancel()
	stop := context.AfterFunc(p.reg.ctx, cancel)
	defer stop()
	if err := p.reg.ctx.Err(); err != nil {
		return nil, err
	}

	// Send the authored node with the Device and remote placement identities.
	resp, err := p.client.Compile(ctx, flowgraph_registry.NewCompileRequest(node))
	if err != nil {
		return nil, err
	}

	// Check declarations, then bind plugin configs to this registration and
	// resolve core configs on the host bus.
	entries := make(map[string]config.Config, len(resp.GetEntries()))
	for name, encoded := range resp.GetEntries() {
		if name == "" || encoded == nil {
			return nil, errors.New("plugin returned an invalid node entry")
		}
		if _, ok := p.configIDs[encoded.GetId()]; !ok {
			return nil, errors.Errorf("plugin emitted undeclared config ID %q", encoded.GetId())
		}
		if _, ok := p.pluginConfigIDs[encoded.GetId()]; ok {
			entries[name] = &pluginConfig{configID: encoded.GetId(), data: bytes.Clone(encoded.GetConfig()), reg: p.reg}
			continue
		}
		resolved, err := encoded.Resolve(ctx, p.b)
		if err != nil {
			return nil, err
		}
		entries[name] = resolved.GetConfig()
	}
	return entries, nil
}

// idSet returns the set of ids.
func idSet(ids []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

// _ is a type assertion.
var _ s4wave_flowgraph.FlowgraphNodeType = (*nodeTypeProxy)(nil)
