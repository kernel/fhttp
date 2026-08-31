// Copyright 2026 The fhttp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	http "github.com/kernel/fhttp"
	"github.com/kernel/fhttp/http2/hpack"
)

// throttledSink caps the rate at which a response body is consumed, simulating
// a slow reader: a proxy relaying to a backpressured client, a rate-limited
// download, etc.
type throttledSink struct{ bytesPerSecond int }

func (s throttledSink) Write(p []byte) (int, error) {
	time.Sleep(time.Duration(len(p)) * time.Second / time.Duration(s.bytesPerSecond))
	return len(p), nil
}

// TestTransportSlowReaderLargeResponse verifies that a response body much
// larger than the stream and connection flow-control windows is delivered in
// full when the application consumes it slowly.
//
// Regression test for a WINDOW_UPDATE accounting bug: Read credited the peer
// for buffered-but-unread body bytes (adding cs.bufPipe.Len() where it should
// subtract it), so the advertised stream window desynced from the real one and
// the transfer died partway with "stream error: ...; FLOW_CONTROL_ERROR".
func TestTransportSlowReaderLargeResponse(t *testing.T) {
	const (
		bodySize       = 48 << 20 // 48 MiB, ~3x the 15.6 MiB connection window
		bytesPerSecond = 12 << 20
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		s := &Server{}
		s.ServeConn(conn, &ServeConnOpts{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", fmt.Sprint(bodySize))
			w.WriteHeader(200)
			chunk := make([]byte, 1<<20)
			for remain := bodySize; remain > 0; remain -= len(chunk) {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
		})})
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tr := &Transport{
		Settings: map[SettingID]uint32{
			SettingInitialWindowSize: 6291456,
		},
		SettingsOrder:     []SettingID{SettingInitialWindowSize},
		ConnectionFlow:    15663105,
		PseudoHeaderOrder: []string{":method", ":authority", ":scheme", ":path"},
	}
	cc, err := tr.NewClientConn(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	req, err := http.NewRequest("GET", "https://example.test/", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := cc.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	n, err := io.Copy(throttledSink{bytesPerSecond: bytesPerSecond}, resp.Body)
	if err != nil {
		t.Fatalf("slow read of %d-byte response failed after %d bytes: %v", bodySize, n, err)
	}
	if n != bodySize {
		t.Fatalf("read %d bytes, want %d", n, bodySize)
	}
}

// TestTransportBodyCloseRaceRefundsConnFlow verifies that closing a response
// body while DATA frames for it are still arriving does not leak
// connection-level flow control credit.
//
// Regression test for a lost-refund race: processData checked cs.didReset
// under cc.mu but wrote to the body pipe after releasing it, while Close
// refunded buffered bytes and only then broke the pipe. Data written in
// between was silently discarded by the broken pipe, and its
// connection-level credit was never returned. Read-path window refreshes
// cannot heal the deficit because they only run when a read returns data,
// so once the window hit zero every later stream on the connection starved
// with zero bytes until the origin reset it. See golang.org/x/net commit
// 9f24bb44 (golang/go#57578).
//
// The peer is a frame-level server that strictly obeys the flow control
// windows the client advertises, so a starved transfer can only mean the
// client failed to refund credit. The race is probabilistic; the abort loop
// amplifies it. The test never fails on correct code and reliably starves
// the canary request on code with the leak.
func TestTransportBodyCloseRaceRefundsConnFlow(t *testing.T) {
	const (
		bodySize   = 256 << 10
		iterations = 200
		frameSize  = 8 << 10
	)

	ct := newClientTester(t)
	ct.tr.Settings = map[SettingID]uint32{
		SettingInitialWindowSize: 65535,
	}
	ct.tr.SettingsOrder = []SettingID{SettingInitialWindowSize}
	ct.tr.ConnectionFlow = 65535

	// If the connection wedges (the failure mode under test), unblock all
	// reads so the test fails fast instead of hitting the suite timeout.
	watchdog := time.AfterFunc(60*time.Second, func() {
		ct.sc.Close()
		ct.cc.Close()
	})
	defer watchdog.Stop()

	ct.client = func() error {
		for i := 0; i <= iterations; i++ {
			req, err := http.NewRequest("GET", "https://dummy.tld/", nil)
			if err != nil {
				return err
			}
			resp, err := ct.tr.RoundTrip(req)
			if err != nil {
				return fmt.Errorf("iteration %d: RoundTrip: %w", i, err)
			}
			if i < iterations {
				// Abort mid-transfer so Close races DATA delivery.
				if _, err := io.CopyN(io.Discard, resp.Body, 4<<10); err != nil {
					resp.Body.Close()
					return fmt.Errorf("iteration %d: read: %w", i, err)
				}
				resp.Body.Close()
				continue
			}
			// Canary: with the connection window intact this completes
			// instantly; with leaked credit it starves with zero bytes,
			// like every later stream on a wedged connection.
			n, err := io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if err != nil {
				return fmt.Errorf("canary read failed after %d of %d bytes (connection-level flow control credit leaked?): %w", n, bodySize, err)
			}
			if n != bodySize {
				return fmt.Errorf("canary read %d bytes, want %d", n, bodySize)
			}
		}
		return nil
	}

	ct.server = func() error {
		ct.greet()
		ct.fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)

		var hbuf bytes.Buffer
		henc := hpack.NewEncoder(&hbuf)

		// The client's connection-level receive window: the 65535 default
		// plus whatever WINDOW_UPDATEs it sends, starting with the
		// ConnectionFlow announcement. Stream windows start at the client's
		// SETTINGS_INITIAL_WINDOW_SIZE.
		connSend := 65535
		streamSend := make(map[uint32]int)
		remaining := make(map[uint32]int)
		var lastStream uint32

		push := func() error {
			for {
				wrote := false
				for id, rem := range remaining {
					n := frameSize
					if n > rem {
						n = rem
					}
					if n > streamSend[id] {
						n = streamSend[id]
					}
					if n > connSend {
						n = connSend
					}
					if n <= 0 {
						continue
					}
					if err := ct.fr.WriteData(id, false, make([]byte, n)); err != nil {
						return err
					}
					remaining[id] -= n
					streamSend[id] -= n
					connSend -= n
					if remaining[id] == 0 {
						if err := ct.fr.WriteData(id, true, nil); err != nil {
							return err
						}
						delete(remaining, id)
						delete(streamSend, id)
					}
					wrote = true
				}
				if !wrote {
					return nil
				}
			}
		}

		for {
			f, err := ct.fr.ReadFrame()
			if err != nil {
				return fmt.Errorf("server ReadFrame: %w", err)
			}
			switch f := f.(type) {
			case *MetaHeadersFrame:
				id := f.StreamID
				lastStream = id
				hbuf.Reset()
				henc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
				henc.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(bodySize)})
				if err := ct.fr.WriteHeaders(HeadersFrameParam{
					StreamID:      id,
					BlockFragment: hbuf.Bytes(),
					EndHeaders:    true,
				}); err != nil {
					return err
				}
				streamSend[id] = 65535
				remaining[id] = bodySize
			case *WindowUpdateFrame:
				if f.StreamID == 0 {
					connSend += int(f.Increment)
				} else if _, ok := streamSend[f.StreamID]; ok {
					streamSend[f.StreamID] += int(f.Increment)
				}
			case *RSTStreamFrame:
				delete(remaining, f.StreamID)
				delete(streamSend, f.StreamID)
			}
			if err := push(); err != nil {
				return err
			}
			// The canary stream is the (iterations+1)-th; once it has been
			// served in full, the script is complete.
			if lastStream == 2*iterations+1 && len(remaining) == 0 {
				return nil
			}
		}
	}

	ct.run()
}

// TestTransportPausedBodiesDoNotExhaustConnectionWindow verifies that response
// bodies which stop being read do not prevent other streams from making
// progress once their DATA has been buffered.
//
// The three paused bodies need more connection credit than the window holds,
// so before connection-level credit was refunded at buffer time this test
// failed with "paused bodies did not fill their stream windows": buffering
// itself stalled once the connection window ran dry.
func TestTransportPausedBodiesDoNotExhaustConnectionWindow(t *testing.T) {
	const (
		streamWindow = 6 << 20
		connWindow   = 15663105
		bodySize     = streamWindow + 1<<20
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		(&Server{}).ServeConn(conn, &ServeConnOpts{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", strconv.Itoa(bodySize))
				w.WriteHeader(http.StatusOK)
				chunk := make([]byte, 16<<10)
				for written := 0; written < bodySize; written += len(chunk) {
					if _, err := w.Write(chunk); err != nil {
						return
					}
				}
			}),
		})
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	tr := &Transport{
		Settings: map[SettingID]uint32{
			SettingInitialWindowSize: streamWindow,
		},
		SettingsOrder:     []SettingID{SettingInitialWindowSize},
		ConnectionFlow:    connWindow,
		PseudoHeaderOrder: []string{":method", ":authority", ":scheme", ":path"},
	}
	cc, err := tr.NewClientConn(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	responses := make([]*http.Response, 0, 4)
	defer func() {
		for _, resp := range responses {
			resp.Body.Close()
		}
	}()

	for i := 0; i < 3; i++ {
		req, err := http.NewRequest(http.MethodGet, "https://example.test/paused/"+strconv.Itoa(i), nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := cc.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		responses = append(responses, resp)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		cc.mu.Lock()
		available := cc.inflow.avail
		pending := cc.unsentConnRefund
		streams := make([]*clientStream, 0, len(cc.streams))
		for _, cs := range cc.streams {
			streams = append(streams, cs)
		}
		cc.mu.Unlock()

		allBuffered := len(streams) == 3
		for _, cs := range streams {
			if cs.bufPipe.Len() > streamWindow {
				t.Fatalf("stream buffered %d bytes, more than its %d byte window", cs.bufPipe.Len(), streamWindow)
			}
			if cs.bufPipe.Len() < streamWindow {
				allBuffered = false
				break
			}
		}
		if allBuffered {
			// Refunds may still be batched in unsentConnRefund rather than
			// added to the announced window; both count as returned credit.
			if available+pending < connWindow {
				t.Fatalf("paused bodies reduced connection window: available=%d pending=%d, want at least %d", available, pending, connWindow)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("paused bodies did not fill their stream windows")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got, want := cc.BufferedUnreadBytes(), int64(3*streamWindow); got != want {
		t.Fatalf("BufferedUnreadBytes = %d, want %d", got, want)
	}

	req, err := http.NewRequest(http.MethodGet, "https://example.test/fourth", nil)
	if err != nil {
		t.Fatal(err)
	}
	fourth, err := cc.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	responses = append(responses, fourth)

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := fourth.Body.Read(buf)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("fourth response body read failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fourth response body remained blocked behind paused bodies")
	}

	cc.Close()
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop after client connection closed")
	}
}
