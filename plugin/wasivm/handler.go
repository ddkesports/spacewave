package wasivm_plugin

import (
	"context"

	"github.com/aperturerobotics/controllerbus/bus"
	"github.com/aperturerobotics/starpc/srpc"
	"github.com/pkg/errors"
	resource_server "github.com/s4wave/spacewave/bldr/resource/server"
	"github.com/s4wave/spacewave/db/world"
	s4wave_objecttype_registry "github.com/s4wave/spacewave/sdk/objecttype/registry"
	s4wave_process "github.com/s4wave/spacewave/sdk/process"
	s4wave_vm "github.com/s4wave/spacewave/sdk/vm"
	sdk_world_engine "github.com/s4wave/spacewave/sdk/world/engine"
	"github.com/sirupsen/logrus"
)

// handler serves the WasiVM ObjectType to the core plugin.
type handler struct {
	le *logrus.Entry
	// b mounts the CDN Spaces of published images.
	b bus.Bus
	// consoles holds the console of each running VM.
	consoles *consoleTable
}

// newHandler constructs a handler.
func newHandler(le *logrus.Entry, b bus.Bus) *handler {
	return &handler{le: le, b: b, consoles: newConsoleTable()}
}

// InvokeObjectType opens a WasiVM resource on the attached engine.
func (h *handler) InvokeObjectType(
	ctx context.Context,
	req *s4wave_objecttype_registry.InvokeObjectTypeRequest,
) (*s4wave_objecttype_registry.InvokeObjectTypeResponse, error) {
	// Require a WasiVM and the engine it lives in.
	if req.GetTypeId() != s4wave_vm.WasiVmTypeID {
		return nil, errors.Errorf("wasivm plugin: unhandled object type %s", req.GetTypeId())
	}
	if req.GetAttachedEngineResourceId() == 0 {
		return nil, errors.New("wasivm plugin: attached engine resource is required")
	}
	resourceCtx, err := resource_server.MustGetResourceClientContext(ctx)
	if err != nil {
		return nil, err
	}

	// Serve execution, the console and the display of the VM on the attached
	// engine.
	engine, err := sdk_world_engine.NewAttachedEngine(ctx, req.GetAttachedEngineResourceId())
	if err != nil {
		return nil, err
	}
	vm := &vmResource{
		le:        h.le.WithField("object-key", req.GetObjectKey()),
		b:         h.b,
		objectKey: req.GetObjectKey(),
		ws:        world.NewEngineWorldState(engine, true),
		consoles:  h.consoles,
	}
	mux := resource_server.NewResourceMux(func(mux srpc.Mux) error {
		if err := s4wave_process.SRPCRegisterPersistentExecutionService(mux, vm); err != nil {
			return err
		}
		if err := s4wave_vm.SRPCRegisterWasiVmConsoleService(mux, &vmConsole{consoles: h.consoles, objectKey: vm.objectKey}); err != nil {
			return err
		}
		if err := s4wave_vm.SRPCRegisterWasiVmDisplayService(mux, &vmDisplay{consoles: h.consoles, objectKey: vm.objectKey}); err != nil {
			return err
		}
		return s4wave_vm.SRPCRegisterWasiVmDownloadService(mux, &vmDownloads{consoles: h.consoles, objectKey: vm.objectKey})
	})
	resourceID, err := resourceCtx.AddResource(mux, engine.Release)
	if err != nil {
		engine.Release()
		return nil, err
	}
	return &s4wave_objecttype_registry.InvokeObjectTypeResponse{ResourceId: resourceID}, nil
}

// wasiVMSchema declares the WasiVM world operations.
var wasiVMSchema = world.NewSchema(s4wave_vm.WasivmTypes)

// _ is a type assertion.
var _ s4wave_objecttype_registry.SRPCObjectTypeHandlerServiceServer = (*handler)(nil)
