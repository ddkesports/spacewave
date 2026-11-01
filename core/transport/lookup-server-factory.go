package transport

import (
	"context"
	"slices"

	"github.com/aperturerobotics/controllerbus/config"
	"github.com/aperturerobotics/controllerbus/controller"
	"github.com/pkg/errors"
	stream_srpc_server_lookup "github.com/s4wave/spacewave/net/stream/srpc/server/lookup"
)

// lookupServerFactory binds core lookup servers to the Session's admission rule.
type lookupServerFactory struct {
	// session owns incoming streams and their admission.
	session *SessionTransport
}

// GetConfigID returns the core lookup server config ID.
func (f *lookupServerFactory) GetConfigID() string { return stream_srpc_server_lookup.ConfigID }

// ConstructConfig constructs a core lookup server config.
func (f *lookupServerFactory) ConstructConfig() config.Config {
	return &stream_srpc_server_lookup.Config{}
}

// GetVersion returns the core lookup server version.
func (f *lookupServerFactory) GetVersion() controller.Version {
	return stream_srpc_server_lookup.Version
}

// Construct creates a lookup server whose streams must pass Session admission.
func (f *lookupServerFactory) Construct(ctx context.Context, conf config.Config, opts controller.ConstructOpts) (controller.Controller, error) {
	// Build the core server on the Session bus that holds its peer and links.
	lookup := conf.(*stream_srpc_server_lookup.Config)
	if len(lookup.GetPeerIds()) != 0 && !slices.Contains(lookup.GetPeerIds(), f.session.GetPeerID().String()) {
		return nil, errors.New("lookup server does not name this Session peer")
	}

	// Route service lookups on the Session bus behind its admitted listeners.
	server, err := stream_srpc_server_lookup.NewController(f.session.GetChildBus(), opts.GetLogger(), lookup)
	if err != nil {
		return nil, err
	}

	// Expose the handler through Session admission rather than its raw directives.
	return &lookupServer{Server: server, session: f.session, protocols: lookup.GetProtocolIds()}, nil
}

var _ controller.Factory = (*lookupServerFactory)(nil)
