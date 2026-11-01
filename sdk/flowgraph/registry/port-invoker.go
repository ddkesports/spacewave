package flowgraph_registry

import (
	"slices"

	"github.com/aperturerobotics/starpc/srpc"
)

// portInvoker limits a plugin port to its declared services.
type portInvoker struct {
	// invoker resolves the service on the plugin bus.
	invoker srpc.Invoker
	// services lists the services this registration grants.
	services []string
}

// InvokeMethod dispatches a declared service and leaves others unimplemented.
func (i *portInvoker) InvokeMethod(serviceID, methodID string, strm srpc.Stream) (bool, error) {
	if !slices.Contains(i.services, serviceID) {
		return false, nil
	}
	return i.invoker.InvokeMethod(serviceID, methodID, strm)
}

var _ srpc.Invoker = (*portInvoker)(nil)
