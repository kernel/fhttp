// Copyright 2026 The fhttp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"net"
	"strconv"
	"testing"
	"time"

	http "github.com/bogdanfinn/fhttp"
)

// TestTransportPausedBodiesDoNotExhaustConnectionWindow verifies that response
// bodies which stop being read do not prevent other streams from making
// progress once their DATA has been buffered.
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
		available := cc.inflow.available()
		streams := make([]*clientStream, 0, len(cc.streams))
		for _, cs := range cc.streams {
			streams = append(streams, cs)
		}
		cc.mu.Unlock()

		allBuffered := len(streams) == 3
		for _, cs := range streams {
			if cs.bufPipe.Len() < streamWindow {
				allBuffered = false
				break
			}
		}
		if allBuffered {
			if available < connWindow {
				t.Fatalf("paused bodies reduced connection window: available=%d, want at least %d", available, connWindow)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("paused bodies did not fill their stream windows")
		}
		time.Sleep(10 * time.Millisecond)
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
