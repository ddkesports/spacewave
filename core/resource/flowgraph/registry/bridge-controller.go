package resource_flowgraph_registry

import (
	"context"

	"github.com/aperturerobotics/controllerbus/bus"
	"github.com/aperturerobotics/controllerbus/controller"
	"github.com/aperturerobotics/controllerbus/controller/resolver"
	"github.com/aperturerobotics/controllerbus/directive"
	s4wave_flowgraph "github.com/s4wave/spacewave/sdk/flowgraph"
)

// BridgeController publishes compile proxies for registered plugin node types,
// and the constructors and factories that run their plugin config IDs inside
// the plugin.
type BridgeController struct {
	// b resolves the compiled controller config constructors.
	b bus.Bus
	// registry retains compiler capabilities and publishes registration changes.
	registry *RegistryResource
}

// NewBridgeController constructs the bridge for a daemon's node type registry.
func NewBridgeController(b bus.Bus, registry *RegistryResource) *BridgeController {
	return &BridgeController{b: b, registry: registry}
}

// GetControllerInfo describes the node type bridge.
func (c *BridgeController) GetControllerInfo() *controller.Info {
	return controller.NewInfo("resource/flowgraph-node-types", controller.MustParseVersion("0.0.1"), "plugin node type compile proxies")
}

// Execute leaves registration watches to the lookup resolvers.
func (c *BridgeController) Execute(context.Context) error {
	return nil
}

// HandleDirective follows registration arrival, withdrawal and replacement
// for node type lookups and for the plugin config IDs' constructors and
// factories.
func (c *BridgeController) HandleDirective(ctx context.Context, di directive.Instance) ([]directive.Resolver, error) {
	// Match standing demand even when its plugin has not registered yet.
	switch dir := di.GetDirective().(type) {
	case s4wave_flowgraph.LookupFlowgraphNodeType:
		typeID := dir.LookupFlowgraphNodeTypeID()
		return c.watch(func() *nodeTypeRegistration { return c.registry.registrations[typeID] }, func(reg *nodeTypeRegistration) directive.Value {
			return NewNodeTypeProxy(c.b, reg.metadata, reg.client, reg.ctx)
		})
	case resolver.LoadConfigConstructorByID:
		configID := dir.LoadConfigConstructorByIDConfigID()
		return c.watch(func() *nodeTypeRegistration { return c.registry.pluginConfigs[configID] }, func(*nodeTypeRegistration) directive.Value {
			return pluginConfigConstructor(configID)
		})
	case resolver.LoadFactoryByConfig:
		conf, ok := dir.LoadFactoryByConfig().(*pluginConfig)
		if !ok {
			return nil, nil
		}
		return c.watch(func() *nodeTypeRegistration { return c.registry.pluginConfigs[conf.configID] }, func(reg *nodeTypeRegistration) directive.Value {
			return &pluginFactory{configID: conf.configID, reg: reg}
		})
	}
	return nil, nil
}

// watch publishes the value for the registration lookup returns, replacing it
// when the registration changes and withdrawing it when none remains.
func (c *BridgeController) watch(lookup func() *nodeTypeRegistration, value func(*nodeTypeRegistration) directive.Value) ([]directive.Resolver, error) {
	return directive.R(directive.NewFuncResolver(func(ctx context.Context, handler directive.ResolverHandler) error {
		var current *nodeTypeRegistration
		var valueID uint32
		for {
			// Read the registration and its change notification together.
			var next *nodeTypeRegistration
			var wait <-chan struct{}
			c.registry.bcast.HoldLock(func(_ func(), getWait func() <-chan struct{}) {
				next = lookup()
				wait = getWait()
			})

			// Withdraw the old value before publishing a replacement.
			if next != current {
				if valueID != 0 {
					handler.RemoveValue(valueID)
					valueID = 0
				}
				current = next
				if next != nil {
					valueID, _ = handler.AddValue(value(next))
				}
			}

			// Keep unresolved demand alive until the registration changes.
			handler.MarkIdle(true)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wait:
			}
			handler.MarkIdle(false)
		}
	}), nil)
}

// Close releases no state because registration Resources retain the compilers.
func (c *BridgeController) Close() error {
	return nil
}

// _ is a type assertion.
var _ controller.Controller = (*BridgeController)(nil)
