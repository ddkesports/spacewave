package stream_packet

import (
	"encoding/binary"
	"io"
	"math"
	"sync"

	protobuf_go_lite "github.com/aperturerobotics/protobuf-go-lite"
	"github.com/pkg/errors"
)

// ErrMessageTooLarge reports a frame rejected before allocating its payload.
var ErrMessageTooLarge = errors.New("packet message too large")

// Session wraps a stream in a session.
type Session struct {
	io.ReadWriteCloser
	sendMtx        sync.Mutex
	readMtx        sync.Mutex
	maxMessageSize uint32
}

// NewSession builds a new session.
func NewSession(
	stream io.ReadWriteCloser,
	maxMessageSize uint32,
) *Session {
	return &Session{
		ReadWriteCloser: stream,
		maxMessageSize:  maxMessageSize,
	}
}

// SendMsg tries to send a message on the wire.
func (s *Session) SendMsg(msg protobuf_go_lite.Message) error {
	return s.SendMsgs(msg)
}

// SendMsgs sends messages in order with one write, so a stream whose writes
// each wait for its transport pays that wait once per batch.
func (s *Session) SendMsgs(msgs ...protobuf_go_lite.Message) error {
	// Measure and bound each serialized message.
	sizes := make([]int, len(msgs))
	total := 0
	for i, msg := range msgs {
		sizes[i] = msg.SizeVT()
		if sizes[i] > math.MaxInt32 {
			return errors.New("message too large: exceeds maximum uint32 value")
		}
		total += sizes[i] + 4
	}

	// Allocate the length-prefixed packets and marshal each payload.
	pktBuf := make([]byte, total)
	off := 0
	for i, msg := range msgs {
		size := sizes[i]
		binary.LittleEndian.PutUint32(pktBuf[off:off+4], uint32(size)) //nolint:gosec // size is bounded by the packet framing limit.
		off += 4
		if _, err := msg.MarshalToSizedBufferVT(pktBuf[off : off+size]); err != nil {
			return err
		}
		off += size
	}

	// Serialize writes under the session send lock.
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	// Write the complete packets to the underlying stream.
	if _, err := s.Write(pktBuf); err != nil {
		return err
	}
	return nil
}

// RecvMsg tries to receive a message on the wire.
func (s *Session) RecvMsg(msg protobuf_go_lite.Message) error {
	// Serialize reads under the session receive lock.
	var hdr [4]byte
	s.readMtx.Lock()
	defer s.readMtx.Unlock()

	// Read the fixed-size message length prefix.
	if _, err := io.ReadFull(s.ReadWriteCloser, hdr[:]); err != nil {
		return err
	}

	// Decode and validate the payload length.
	messageLen := binary.LittleEndian.Uint32(hdr[:])
	if messageLen > 0 {
		if messageLen > s.maxMessageSize {
			return errors.Wrapf(ErrMessageTooLarge, "message length %d exceeds limit %d", messageLen, s.maxMessageSize)
		}

		// Read and decode the payload bytes.
		data := make([]byte, messageLen)
		if _, err := io.ReadFull(s.ReadWriteCloser, data); err != nil {
			return err
		}

		return msg.UnmarshalVT(data)
	}

	// Reset the destination for an empty message.
	msg.Reset()
	return nil
}
