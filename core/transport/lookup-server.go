package transport

import (
	"context"

	"github.com/aperturerobotics/controllerbus/controller"
	"github.com/aperturerobotics/controllerbus/directive"
	"github.com/s4wave/spacewave/net/protocol"
	stream_srpc_server "github.com/s4wave/spacewave/net/stream/srpc/server"
)

// lookupServer serves the core lookup handler through the Session's admission rule.
type lookupServer struct {
	// Server supplies SRPC lookup dispatch and controller metadata.
	*stream_srpc_server.Server
	// session owns the admission rule and admitted stream lifetime.
	session *SessionTransport
	// protocols contains the configured port protocols.
	protocols []string
	// releases withdraws each admitted listener on Close.
	releases []func()
}

// Execute installs listeners that admit incoming peers before dispatch.
func (s *lookupServer) Execute(context.Context) error {
	for _, id := range s.protocols {
		release, err := s.session.ServeAuthorized(protocol.ID(id), s.Server, nil)
		if err != nil {
			return err
		}
		s.releases = append(s.releases, release)
	}
	return nil
}

// HandleDirective leaves stream resolution to the authorized listeners.
func (s *lookupServer) HandleDirective(context.Context, directive.Instance) ([]directive.Resolver, error) {
	return nil, nil
}

// Close withdraws listeners and releases the core lookup server.
func (s *lookupServer) Close() error {
	for _, release := range s.releases {
		release()
	}
	return s.Server.Close()
}

var _ controller.Controller = (*lookupServer)(nil)
