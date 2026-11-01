package resource_flowgraph_registry

import (
	"context"
	"slices"

	"github.com/aperturerobotics/starpc/rpcstream"
	"github.com/aperturerobotics/starpc/srpc"
	flowgraph_registry "github.com/s4wave/spacewave/sdk/flowgraph/registry"
)

// pluginPortInvoker forwards a port's calls for one registration lifetime.
type pluginPortInvoker struct {
	// reg owns the plugin capability and cancellation.
	reg *nodeTypeRegistration
	// serverID selects the placed node's port on the plugin bus.
	serverID string
}

// InvokeMethod forwards one call until the caller or registration ends.
func (i *pluginPortInvoker) InvokeMethod(serviceID, methodID string, strm srpc.Stream) (bool, error) {
	// Refuse services outside the grant even if the plugin bus implements them.
	if !slices.Contains(i.reg.metadata.GetPluginServiceIds(), serviceID) {
		return false, nil
	}

	// End active calls when the plugin registration withdraws.
	ctx, cancel := context.WithCancel(strm.Context())
	defer cancel()
	stop := context.AfterFunc(i.reg.ctx, cancel)
	defer stop()
	if err := i.reg.ctx.Err(); err != nil {
		return false, err
	}

	// Carry SRPC framing over the retained compiler capability.
	service := flowgraph_registry.NewSRPCNodeTypeResourceServiceClient(i.reg.client)
	client := rpcstream.NewRpcStreamClient(service.ServeRpc, i.serverID, true)
	return srpc.NewClientInvoker(client).InvokeMethod(serviceID, methodID, srpc.NewStreamWithContext(strm, ctx))
}

var _ srpc.Invoker = (*pluginPortInvoker)(nil)
