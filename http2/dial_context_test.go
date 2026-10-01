// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	tls "github.com/bogdanfinn/utls"
	http "github.com/kernel/fhttp"
	"github.com/kernel/fhttp/httptrace"
)

type dialContextTestKey struct{}

func TestTransportDialTLSContextPrecedence(t *testing.T) {
	wantErr := errors.New("dial failed")
	ctx := context.WithValue(context.Background(), dialContextTestKey{}, "observation")
	for _, tc := range []struct {
		name        string
		contextHook bool
	}{{"legacy", false}, {"context", true}} {
		t.Run(tc.name, func(t *testing.T) {
			var legacyCalled, contextCalled bool
			tr := &Transport{DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				legacyCalled = true
				return nil, wantErr
			}}
			if tc.contextHook {
				tr.DialTLSContext = func(got context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
					contextCalled = true
					if got != ctx || got.Value(dialContextTestKey{}) != "observation" {
						t.Error("dial lost request context")
					}
					if network != "tcp" || addr != "origin.test:443" || cfg.ServerName != "origin.test" {
						t.Errorf("unexpected dial arguments: %s %s %s", network, addr, cfg.ServerName)
					}
					return nil, wantErr
				}
			}
			_, err := tr.dialClientConn(ctx, "origin.test:443", false)
			if err != wantErr || contextCalled != tc.contextHook || legacyCalled == tc.contextHook {
				t.Fatalf("err=%v, contextCalled=%v, legacyCalled=%v", err, contextCalled, legacyCalled)
			}
		})
	}
}

func TestClientConnPoolSharedDialContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), dialContextTestKey{}, "first"), time.Hour)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", "https://origin.test/", nil)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	defer close(release)
	wantErr := errors.New("dial failed")
	var calls atomic.Int64
	pool := &clientConnPool{t: &Transport{DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
		calls.Add(1)
		started <- ctx
		<-release
		return nil, wantErr
	}}}
	result := make(chan error, 1)
	go func() {
		_, err := pool.GetClientConn(req, "origin.test:443")
		result <- err
	}()
	var dialCtx context.Context
	select {
	case dialCtx = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("dial did not start")
	}
	cancel()
	if dialCtx.Value(dialContextTestKey{}) != "first" || dialCtx.Err() != nil || dialCtx.Done() != nil {
		t.Fatal("shared dial must retain values without request cancellation")
	}
	if _, ok := dialCtx.Deadline(); ok {
		t.Fatal("shared dial retained request deadline")
	}
	joining := make(chan struct{})
	secondCtx := httptrace.WithClientTrace(context.WithValue(context.Background(), dialContextTestKey{}, "second"), &httptrace.ClientTrace{
		GetConn: func(string) { close(joining) },
	})
	secondReq := req.WithContext(secondCtx)
	secondResult := make(chan error, 1)
	go func() {
		_, err := pool.GetClientConn(secondReq, "origin.test:443")
		secondResult <- err
	}()
	select {
	case <-joining:
	case <-time.After(5 * time.Second):
		t.Fatal("second request did not join")
	}
	// GetConn runs under the pool lock; taking it confirms the second request
	// has joined the in-flight dial before we release it.
	pool.mu.Lock()
	pool.mu.Unlock()
	if calls.Load() != 1 {
		t.Fatal("concurrent requests must share the initiating dial")
	}
	release <- struct{}{}
	for _, result := range []<-chan error{result, secondResult} {
		select {
		case err := <-result:
			if err != wantErr {
				t.Fatalf("GetClientConn error = %v; want %v", err, wantErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("shared dial did not complete")
		}
	}
}

func TestClientConnPoolSingleUseDialContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", "https://origin.test/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Close = true
	var called bool
	pool := &clientConnPool{t: &Transport{DialTLSContext: func(got context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
		called = true
		if got != ctx || got.Err() != context.Canceled {
			t.Error("single-use dial must retain request cancellation")
		}
		if _, ok := got.Deadline(); !ok {
			t.Error("single-use dial lost request deadline")
		}
		return nil, got.Err()
	}}}
	_, err = pool.GetClientConn(req, "origin.test:443")
	if !called || err != context.Canceled {
		t.Fatalf("called=%v, err=%v", called, err)
	}
}

func TestTransportDialTLSContextConnectionReuse(t *testing.T) {
	st := newServerTester(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}, optOnlyServer)
	defer st.Close()
	var calls atomic.Int64
	tr := &Transport{DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
		calls.Add(1)
		if ctx.Value(dialContextTestKey{}) != "first" {
			t.Error("dial did not receive the initiating request's values")
		}
		cfg.InsecureSkipVerify = true
		return tls.Dial(network, addr, cfg)
	}}
	defer tr.CloseIdleConnections()
	for _, value := range []string{"first", "second"} {
		ctx := context.WithValue(context.Background(), dialContextTestKey{}, value)
		req, err := http.NewRequestWithContext(ctx, "GET", st.ts.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header[http.PHeaderOrderKey] = []string{":method", ":authority", ":scheme", ":path"}
		resp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("dials = %d; want 1", calls.Load())
	}
}
