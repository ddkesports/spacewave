package transport_quic

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/s4wave/spacewave/net/crypto"
	p2ptls "github.com/s4wave/spacewave/net/crypto/tls"
	"github.com/s4wave/spacewave/net/peer"
	"github.com/s4wave/spacewave/net/stream"
	"github.com/sirupsen/logrus"
)

// TestParallelLinksKeepDistinctIdentities exercises two authenticated QUIC
// connections with identical endpoints, as manual and signaled WebRTC use.
func TestParallelLinksKeepDistinctIdentities(t *testing.T) {
	// Prepare authenticated peers on two local UDP endpoints.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	le := logrus.NewEntry(logrus.New())
	var identities [2]*p2ptls.Identity
	var peers [2]peer.ID
	var endpoints [2]net.PacketConn
	for i := range identities {
		// Generate the endpoint signing key and TLS identity.
		key, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		identities[i], err = p2ptls.NewIdentity(key)
		if err != nil {
			t.Fatal(err)
		}

		// Bind the peer identity to a local UDP endpoint.
		peers[i], err = peer.IDFromPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		endpoints[i], err = net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer endpoints[i].Close()
	}

	// Start a listener and dialer that reuse the UDP endpoints.
	opts := &Opts{DisablePathMtuDiscovery: true}
	listener, err := quic.Listen(endpoints[1], BuildIncomingTlsConf(identities[1], peers[0]), BuildQuicConfig(opts))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dialer := &quic.Transport{Conn: endpoints[0]}
	defer dialer.Close()

	// Establish independent links for two sessions at the same endpoints.
	var links [2][2]*Link
	for i := range links {
		// Dial the authenticated listener over the shared outgoing transport.
		outgoing, _, err := DialSessionViaTransport(ctx, le, opts, dialer, identities[0], endpoints[1].LocalAddr(), peers[1])
		if err != nil {
			t.Fatal(err)
		}
		defer outgoing.CloseWithError(0, "")

		// Accept the listener session and retain it until test cleanup.
		incoming, err := listener.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer incoming.CloseWithError(0, "")

		// Wrap both session ends in links with their local peer identities.
		for side, conn := range []*quic.Conn{outgoing, incoming} {
			links[i][side], err = NewLink(ctx, le, opts, 0, peers[side], endpoints[side].LocalAddr(), conn, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer links[i][side].Close()
		}
	}

	// Verify distinct link identities before retiring the first session.
	for side := range peers {
		if links[0][side].GetUUID() == links[1][side].GetUUID() {
			t.Fatal("independent connections share a link identity")
		}
		if links[0][side].RemoteAddr().String() != links[1][side].RemoteAddr().String() {
			t.Fatal("fixture did not reuse the remote endpoint")
		}
		if err := links[0][side].Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Retiring one connection leaves the other connection usable.
	sent, err := links[1][0].OpenStream(stream.OpenOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer sent.Close()
	if _, err := sent.Write([]byte("still connected")); err != nil {
		t.Fatal(err)
	}

	// Receive the surviving session stream and verify its payload.
	received, _, err := links[1][1].AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	defer received.Close()
	data := make([]byte, len("still connected"))
	if _, err := io.ReadFull(received, data); err != nil {
		t.Fatal(err)
	}
	if string(data) != "still connected" {
		t.Fatalf("received %q", data)
	}
}

// TestCloseReleasesBlockedWrite closes a stream whose Write waits on flow
// control credit from a peer that stopped reading, and checks the Write returns.
func TestCloseReleasesBlockedWrite(t *testing.T) {

	// Open a stream the peer accepts but never reads past its first byte.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	links := newLinkPair(ctx, t, &Opts{DisablePathMtuDiscovery: true})
	strm, err := links[0].OpenStream(stream.OpenOpts{})
	if err != nil {
		t.Fatal(err)
	}

	// The peer sees the stream once it carries data.
	if _, err := strm.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	accepted, _, err := links[1].AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()

	// Fill the peer's receive window so the next Write blocks, then close.
	written := make(chan error, 1)
	go func() {
		_, err := strm.Write(make([]byte, 64<<20))
		written <- err
	}()
	select {
	case err := <-written:
		t.Fatalf("write past the receive window returned early: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	_ = strm.Close()
	select {
	case err := <-written:
		if err == nil {
			t.Fatal("blocked write succeeded after close")
		}
	case <-ctx.Done():
		t.Fatal("close left the write blocked")
	}
}
