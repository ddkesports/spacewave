package s4wave_flowgraph

import (
	"github.com/pkg/errors"
	"github.com/s4wave/spacewave/net/protocol"
)

// NodePortProtocolID addresses a stream port of one placed Flowgraph node.
func NodePortProtocolID(flowgraphKey, nodeID, port string) (protocol.ID, error) {
	// Require the graph's canonical key and unambiguous endpoint segments.
	if _, err := ParseFlowgraphObjectKey(flowgraphKey); err != nil {
		return "", err
	}
	if !validID(nodeID) || !validID(port) {
		return "", errors.New("node and port must be nonempty path segments")
	}

	// Keep the protocol stable across Device placement and plugin replacement.
	return protocol.ID("spacewave/" + flowgraphKey + "/" + nodeID + "/" + port), nil
}
