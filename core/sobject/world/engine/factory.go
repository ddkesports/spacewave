package sobject_world_engine

import (
	"context"

	"github.com/aperturerobotics/controllerbus/bus"
	"github.com/aperturerobotics/controllerbus/config"
	"github.com/aperturerobotics/controllerbus/controller"
	transform_all "github.com/s4wave/spacewave/db/block/transform/all"
)

// ControllerID identifies the block graph engine controller.
const ControllerID = "sobject/world/engine"

// Version is the controller version.
var Version = controller.MustParseVersion("0.0.1")

// Factory constructs a world engine controller.
type Factory struct {
	// bus is the controller bus
	bus bus.Bus
	// opTypes resolves the operation types of each engine.
	opTypes OpTypes
}

// NewFactory builds a world block engine factory.
func NewFactory(bus bus.Bus) *Factory {
	return &Factory{bus: bus}
}

// NewFactoryWithOpTypes builds a world block engine factory whose engines
// resolve operation types with opTypes. They are set at construction, so they
// cover the first replay.
func NewFactoryWithOpTypes(bus bus.Bus, opTypes OpTypes) *Factory {
	return &Factory{bus: bus, opTypes: opTypes}
}

// GetConfigID returns the configuration ID for the controller.
func (t *Factory) GetConfigID() string {
	return ConfigID
}

// GetControllerID returns the unique ID for the controller.
func (t *Factory) GetControllerID() string {
	return ControllerID
}

// ConstructConfig constructs an instance of the controller configuration.
func (t *Factory) ConstructConfig() config.Config {
	return &Config{}
}

// Construct constructs the associated controller given configuration.
func (t *Factory) Construct(
	ctx context.Context,
	conf config.Config,
	opts controller.ConstructOpts,
) (controller.Controller, error) {
	// Construct the controller with the default block transforms.
	le := opts.GetLogger()
	cc := conf.(*Config)
	sfs := transform_all.BuildFactorySet()
	ctrl, err := NewController(le, t.bus, cc, sfs)
	if err != nil {
		return nil, err
	}

	// Set the operation types before the controller executes.
	ctrl.SetOpTypes(t.opTypes)
	return ctrl, nil
}

// GetVersion returns the version of this controller.
func (t *Factory) GetVersion() controller.Version {
	return Version
}

// _ is a type assertion
var _ controller.Factory = (*Factory)(nil)
