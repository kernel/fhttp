package http2

import (
	"sync/atomic"
	"time"
)

// DebugEvent describes an inbound HTTP/2 event that may explain a stream or
// connection failure. DebugLog callbacks run on the connection read loop and
// should return quickly.
type DebugEvent struct {
	Kind string

	ConnectionID  uint64
	ConnectionAge time.Duration
	RemoteAddr    string
	OpenStreams   int

	FrameType     string
	LastFrameType string
	StreamID      uint32
	ErrorCode     ErrCode
	Error         error
	ErrorDetail   error
}

var nextClientConnID uint64

func (cc *ClientConn) debugEvent(kind, frameType string, streamID uint32, code ErrCode, err, detail error) {
	if cc.t.DebugLog == nil {
		return
	}

	cc.mu.Lock()
	openStreams := len(cc.streams)
	lastFrameType := cc.lastFrameType
	cc.mu.Unlock()

	remoteAddr := ""
	if cc.tconn != nil && cc.tconn.RemoteAddr() != nil {
		remoteAddr = cc.tconn.RemoteAddr().String()
	}
	cc.t.DebugLog(DebugEvent{
		Kind:          kind,
		ConnectionID:  cc.connectionID,
		ConnectionAge: time.Since(cc.createdAt),
		RemoteAddr:    remoteAddr,
		OpenStreams:   openStreams,
		FrameType:     frameType,
		LastFrameType: lastFrameType,
		StreamID:      streamID,
		ErrorCode:     code,
		Error:         err,
		ErrorDetail:   detail,
	})
}

func (cc *ClientConn) noteFrame(ft FrameType) {
	cc.mu.Lock()
	cc.lastFrameType = ft.String()
	cc.mu.Unlock()
}

func nextConnectionID() uint64 {
	return atomic.AddUint64(&nextClientConnID, 1)
}
