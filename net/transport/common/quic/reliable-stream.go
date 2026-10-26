package transport_quic

import (
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/s4wave/spacewave/net/stream"
)

// reliableStream is a reliable stream on a QUIC link.
//
// A QUIC stream's Close ends only the send direction: a pending Read keeps
// blocking until the peer closes its side, and a pending Write keeps waiting
// for flow control credit the peer may never grant. reliableStream's Close also
// cancels the receive direction and expires the pending Write, as the
// stream.Stream contract requires, so shutdown never waits on the peer. Data
// already written is still delivered before the FIN. CloseWrite keeps the
// half-close for callers that read a reply after they finish writing.
type reliableStream struct {
	*quic.Stream
}

// Close cancels the receive direction and closes the send direction. A pending
// Read or Write returns an error.
func (s reliableStream) Close() error {
	s.CancelRead(0)
	_ = s.SetWriteDeadline(time.Now())
	return s.Stream.Close()
}

// CloseWrite closes the send direction. Reads continue until the peer closes.
func (s reliableStream) CloseWrite() error {
	return s.Stream.Close()
}

// _ is a type assertion
var _ stream.Stream = reliableStream{}
