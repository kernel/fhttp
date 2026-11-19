// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"testing"
)

func TestPipeClose(t *testing.T) {
	var p pipe
	p.b = new(bytes.Buffer)
	a := errors.New("a")
	b := errors.New("b")
	p.CloseWithError(a)
	p.CloseWithError(b)
	_, err := p.Read(make([]byte, 1))
	if err != a {
		t.Errorf("err = %v want %v", err, a)
	}
}

func TestPipeDoneChan(t *testing.T) {
	var p pipe
	done := p.Done()
	select {
	case <-done:
		t.Fatal("done too soon")
	default:
	}
	p.CloseWithError(io.EOF)
	select {
	case <-done:
	default:
		t.Fatal("should be done")
	}
}

func TestPipeDoneChan_ErrFirst(t *testing.T) {
	var p pipe
	p.CloseWithError(io.EOF)
	done := p.Done()
	select {
	case <-done:
	default:
		t.Fatal("should be done")
	}
}

func TestPipeDoneChan_Break(t *testing.T) {
	var p pipe
	done := p.Done()
	select {
	case <-done:
		t.Fatal("done too soon")
	default:
	}
	p.BreakWithError(io.EOF)
	select {
	case <-done:
	default:
		t.Fatal("should be done")
	}
}

func TestPipeDoneChan_Break_ErrFirst(t *testing.T) {
	var p pipe
	p.BreakWithError(io.EOF)
	done := p.Done()
	select {
	case <-done:
	default:
		t.Fatal("should be done")
	}
}

func TestPipeCloseWithError(t *testing.T) {
	p := &pipe{b: new(bytes.Buffer)}
	const body = "foo"
	io.WriteString(p, body)
	a := errors.New("test error")
	p.CloseWithError(a)
	all, err := ioutil.ReadAll(p)
	if string(all) != body {
		t.Errorf("read bytes = %q; want %q", all, body)
	}
	if err != a {
		t.Logf("read error = %v, %v", err, a)
	}
	if p.Len() != 0 {
		t.Errorf("pipe should have 0 unread bytes")
	}
	// Read and Write should fail.
	if n, err := p.Write([]byte("abc")); err != errClosedPipeWrite || n != 0 {
		t.Errorf("Write(abc) after close\ngot %v, %v\nwant 0, %v", n, err, errClosedPipeWrite)
	}
	if n, err := p.Read(make([]byte, 1)); err == nil || n != 0 {
		t.Errorf("Read() after close\ngot %v, nil\nwant 0, %v", n, errClosedPipeWrite)
	}
	if p.Len() != 0 {
		t.Errorf("pipe should have 0 unread bytes")
	}
}

func TestPipeBreakWithError(t *testing.T) {
	p := &pipe{b: new(bytes.Buffer)}
	io.WriteString(p, "foo")
	a := errors.New("test err")
	p.BreakWithError(a)
	all, err := ioutil.ReadAll(p)
	if string(all) != "" {
		t.Errorf("read bytes = %q; want empty string", all)
	}
	if err != a {
		t.Logf("read error = %v, %v", err, a)
	}
	if p.b != nil {
		t.Errorf("buffer should be nil after BreakWithError")
	}
	if p.Len() != 3 {
		t.Errorf("pipe should have 3 unread bytes")
	}
	// Write should fail.
	if n, err := p.Write([]byte("abc")); err != errClosedPipeWrite || n != 0 {
		t.Errorf("Write(abc) after break\ngot %v, %v\nwant 0, errClosedPipeWrite", n, err)
	}
	if p.b != nil {
		t.Errorf("buffer should be nil after Write")
	}
	if p.Len() != 3 {
		t.Errorf("pipe should have 3 unread bytes")
	}
	// Read should fail.
	if n, err := p.Read(make([]byte, 1)); err == nil || n != 0 {
		t.Errorf("Read() after close\ngot %v, nil\nwant 0, not nil", n)
	}
}

func TestPipeSetBufferAfterClose(t *testing.T) {
	var p pipe
	closeErr := errors.New("closed early")
	p.CloseWithError(closeErr)
	p.setBuffer(new(bytes.Buffer))

	if p.b != nil {
		t.Fatalf("buffer should remain nil after CloseWithError")
	}
	if p.Err() != closeErr {
		t.Fatalf("Err() = %v, want %v", p.Err(), closeErr)
	}
	if p.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", p.Len())
	}
	if n, err := p.Write([]byte("abc")); err != errClosedPipeWrite || n != 0 {
		t.Fatalf("Write() = (%d, %v), want (0, %v)", n, err, errClosedPipeWrite)
	}
	if n, err := p.Read(make([]byte, 1)); err != closeErr || n != 0 {
		t.Fatalf("Read() = (%d, %v), want (0, %v)", n, err, closeErr)
	}
}

func TestPipeSetBufferAfterBreak(t *testing.T) {
	var p pipe
	breakErr := errors.New("broken early")
	p.BreakWithError(breakErr)
	p.setBuffer(new(bytes.Buffer))

	if p.b != nil {
		t.Fatalf("buffer should remain nil after BreakWithError")
	}
	if p.Err() != breakErr {
		t.Fatalf("Err() = %v, want %v", p.Err(), breakErr)
	}
	if p.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", p.Len())
	}
	if n, err := p.Write([]byte("abc")); err != errClosedPipeWrite || n != 0 {
		t.Fatalf("Write() = (%d, %v), want (0, %v)", n, err, errClosedPipeWrite)
	}
	if n, err := p.Read(make([]byte, 1)); err != breakErr || n != 0 {
		t.Fatalf("Read() = (%d, %v), want (0, %v)", n, err, breakErr)
	}
}
