// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tls "github.com/bogdanfinn/utls"
	http "github.com/kernel/fhttp"
	"github.com/kernel/fhttp/httptrace"
)

type dialContextTestKey struct{}

type dialWaitBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *dialWaitBody) Close() error {
	b.closed.Store(true)
	return nil
}

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
	select {
	case err := <-result:
		if err != context.Canceled {
			t.Fatalf("canceled waiter error = %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not leave the shared dial")
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
	select {
	case err := <-secondResult:
		if err != wantErr {
			t.Fatalf("GetClientConn error = %v; want %v", err, wantErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shared dial did not complete")
	}
}

func TestTransportCanceledDialWaiterDoesNotSendRequest(t *testing.T) {
	for _, cancelLeader := range []bool{true, false} {
		t.Run(fmt.Sprint("cancelLeader=", cancelLeader), func(t *testing.T) {
			var requests atomic.Int64
			st := newServerTester(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path == "/canceled" {
					t.Error("canceled request reached server")
				}
				w.Write([]byte("ok"))
			}, optOnlyServer)
			defer st.Close()
			started := make(chan context.Context, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var dials atomic.Int64
			tr := &Transport{DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
				dials.Add(1)
				started <- ctx
				<-release
				cfg.InsecureSkipVerify = true
				return tls.Dial(network, addr, cfg)
			}}
			defer tr.CloseIdleConnections()
			canceledCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &dialWaitBody{Reader: strings.NewReader("payload")}
			run := func(ctx context.Context, path string) <-chan error {
				result := make(chan error, 1)
				go func() {
					req, err := http.NewRequestWithContext(ctx, "GET", st.ts.URL+path, nil)
					if path == "/canceled" && err == nil {
						req.Method = "POST"
						req.Body = body
						req.ContentLength = -1
					}
					if err == nil {
						req.Header[http.PHeaderOrderKey] = []string{":method", ":authority", ":scheme", ":path"}
						var resp *http.Response
						resp, err = tr.RoundTrip(req)
						if err == nil {
							_, err = io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
						}
					}
					result <- err
				}()
				return result
			}
			leaderCtx, leaderPath := canceledCtx, "/canceled"
			followerCtx, followerPath := context.Background(), "/survivor"
			if !cancelLeader {
				leaderCtx, followerCtx = followerCtx, leaderCtx
				leaderPath, followerPath = followerPath, leaderPath
			}
			leader := run(leaderCtx, leaderPath)
			var dialCtx context.Context
			select {
			case dialCtx = <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("dial did not start")
			}
			joining := make(chan struct{})
			followerCtx = httptrace.WithClientTrace(followerCtx, &httptrace.ClientTrace{GetConn: func(string) { close(joining) }})
			follower := run(followerCtx, followerPath)
			select {
			case <-joining:
			case <-time.After(5 * time.Second):
				t.Fatal("follower did not join")
			}
			pool := tr.connPool().(*clientConnPool)
			pool.mu.Lock()
			pool.mu.Unlock()
			canceled, survivor := leader, follower
			if !cancelLeader {
				canceled, survivor = follower, leader
			}
			cancel()
			select {
			case err := <-canceled:
				if err != context.Canceled {
					t.Fatalf("canceled RoundTrip error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled RoundTrip waited for dial completion")
			}
			if !body.closed.Load() {
				t.Fatal("canceled request body was not closed")
			}
			if dialCtx.Err() != nil {
				t.Fatal("waiter canceled the shared dial")
			}
			unblock()
			for _, result := range []<-chan error{survivor, run(context.Background(), "/reuse")} {
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("live request did not complete")
				}
			}
			if requests.Load() != 2 || dials.Load() != 1 {
				t.Fatalf("requests=%d, dials=%d; want 2 requests on one connection", requests.Load(), dials.Load())
			}
		})
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

func TestConfiguredTransportPreservesBodyOnCacheMiss(t *testing.T) {
	st := newServerTester(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	}, optOnlyServer)
	defer st.Close()
	tr := &http.Transport{TLSClientConfig: tlsConfigInsecure}
	if err := ConfigureTransport(tr); err != nil {
		t.Fatal(err)
	}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequest("POST", st.ts.URL, io.NopCloser(strings.NewReader("payload")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header[http.PHeaderOrderKey] = []string{":method", ":authority", ":scheme", ":path"}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != "payload" {
		t.Fatalf("response body=%q, err=%v; want payload", got, err)
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
