package http2

import (
	"bufio"
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"
)

type pingWriteFunc func([]byte) (int, error)

func (f pingWriteFunc) Write(p []byte) (int, error) { return f(p) }

func newPingTestConn(writer io.Writer) *ClientConn {
	cc := &ClientConn{pings: make(map[[8]byte]chan struct{}), readerDone: make(chan struct{})}
	cc.bw = bufio.NewWriter(writer)
	cc.fr = NewFramer(cc.bw, nil)
	return cc
}

func TestPingFailureDetails(t *testing.T) {
	checkNotAckTimeout := func(t *testing.T, details HealthCheckFailureDetails) {
		t.Helper()
		var timeout PingAckTimeoutError
		if err := details.streamError(); errors.As(err, &timeout) || err.Error() != "http2: client connection lost" {
			t.Fatalf("unexpected stream error: %T %v", err, err)
		}
	}
	t.Run("flush failure", func(t *testing.T) {
		cause := errors.New("write failed")
		cc := newPingTestConn(pingWriteFunc(func([]byte) (int, error) { return 0, cause }))
		details, err := cc.ping(context.Background())
		if err != cause || details.Phase != "flush" || details.PingWriteCompleted || details.AckWait != 0 {
			t.Fatalf("ping = (%+v, %v)", details, err)
		}
		checkNotAckTimeout(t, details)
		if details.WriteDuration <= 0 || details.TotalDuration < details.WriteDuration {
			t.Fatalf("invalid durations: %+v", details)
		}
	})
	t.Run("reader failure", func(t *testing.T) {
		for _, cause := range []error{io.EOF, context.DeadlineExceeded} {
			cc := newPingTestConn(io.Discard)
			cc.readerErr = cause
			close(cc.readerDone)
			details, err := cc.ping(context.Background())
			checkNotAckTimeout(t, details)
			if err != cause || details.Phase != "ack_wait" || !details.PingWriteCompleted || details.DeadlineExpiredBeforeAckWait {
				t.Fatalf("ping = (%+v, %v)", details, err)
			}
		}
	})
	t.Run("deadline wins a tie", func(t *testing.T) {
		for _, signal := range []string{"ack", "reader"} {
			t.Run(signal, func(t *testing.T) {
				ack, readerDone := make(chan struct{}), make(chan struct{})
				if signal == "ack" {
					close(ack)
				} else {
					close(readerDone)
				}
				var details HealthCheckFailureDetails
				details.noteAckDeadline(time.Now(), ack, readerDone)
				checkNotAckTimeout(t, details)
			})
		}
	})
	t.Run("blocked flush", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		cc := newPingTestConn(pingWriteFunc(func(p []byte) (int, error) {
			<-ctx.Done()
			return len(p), nil
		}))
		details, err := cc.ping(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || !details.PingWriteCompleted || !details.DeadlineExpiredBeforeAckWait {
			t.Fatalf("ping = (%+v, %v)", details, err)
		}
		checkNotAckTimeout(t, details)
		if details.WriteDuration < 15*time.Millisecond || details.Phase != "ack_wait" {
			t.Fatalf("blocked flush was not measured: %+v", details)
		}
	})
	t.Run("blocked write lock", func(t *testing.T) {
		cc := newPingTestConn(io.Discard)
		cc.wmu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		result := make(chan HealthCheckFailureDetails, 1)
		go func() {
			details, err := cc.ping(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("ping error = %v", err)
			}
			result <- details
		}()
		// Wait for the PING to register before holding its write lock past the deadline.
		for {
			cc.mu.Lock()
			registered := len(cc.pings) != 0
			cc.mu.Unlock()
			if registered || ctx.Err() != nil {
				break
			}
			runtime.Gosched()
		}
		<-ctx.Done()
		cc.wmu.Unlock()
		select {
		case details := <-result:
			checkNotAckTimeout(t, details)
			if details.WriteLockWait < 15*time.Millisecond || !details.PingWriteCompleted || !details.DeadlineExpiredBeforeAckWait {
				t.Fatalf("blocked write lock was not measured: %+v", details)
			}
		case <-time.After(time.Second):
			t.Fatal("ping did not finish after releasing write lock")
		}
	})
}
