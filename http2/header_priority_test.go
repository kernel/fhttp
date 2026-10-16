package http2

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	http "github.com/kernel/fhttp"
	"github.com/kernel/fhttp/http2/hpack"
)

func TestTransportHeaderPriorityFunc(t *testing.T) {
	static := PriorityParam{Weight: 15}
	perRequest := PriorityParam{Exclusive: true, Weight: 219}

	ct := newClientTester(t)
	ct.tr.HeaderPriority = &static
	ct.tr.HeaderPriorityFunc = func(req *http.Request) *PriorityParam {
		if req.URL.Path == "/fallback" {
			return nil
		}
		return &perRequest
	}
	paths := []string{"/per-request", "/fallback"}
	want := []PriorityParam{perRequest, static}

	ct.client = func() error {
		for _, path := range paths {
			req, _ := http.NewRequest("GET", "https://dummy.tld"+path, nil)
			res, err := ct.tr.RoundTrip(req)
			if err != nil {
				return fmt.Errorf("RoundTrip %s: %v", path, err)
			}
			res.Body.Close()
		}
		return nil
	}
	ct.server = func() error {
		ct.greet()
		var buf bytes.Buffer
		enc := hpack.NewEncoder(&buf)
		for i := 0; i < len(want); {
			f, err := ct.fr.ReadFrame()
			if err != nil {
				return err
			}
			hf, ok := f.(*HeadersFrame)
			if !ok {
				continue
			}
			if hf.Priority != want[i] {
				return fmt.Errorf("%s: HEADERS priority = %+v; want %+v", paths[i], hf.Priority, want[i])
			}
			buf.Reset()
			enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
			if err := ct.fr.WriteHeaders(HeadersFrameParam{
				StreamID:      hf.StreamID,
				BlockFragment: buf.Bytes(),
				EndStream:     true,
				EndHeaders:    true,
			}); err != nil {
				return err
			}
			i++
		}
		return nil
	}
	ct.run()
}

func TestTransportPriorityDependencies(t *testing.T) {
	weights := map[string]uint8{"/u0": 255, "/u0b": 255, "/u1": 219, "/u2": 182, "/u2b": 182, "/u3": 146}
	type step struct {
		path string
		dep  uint32
	}
	// Streams 1 (/u0) and 5 (/u1) finish before the last two requests, so
	// those depend only on streams 3 and 7.
	steps := []step{{"/u0", 0}, {"/u2", 1}, {"/u1", 1}, {"/u2b", 5}, {"/u3", 7}, {"/u0b", 0}}
	const finishAfter = 4

	ct := newClientTester(t)
	ct.tr.PriorityDependencies = true
	ct.tr.HeaderPriorityFunc = func(req *http.Request) *PriorityParam {
		return &PriorityParam{Weight: weights[req.URL.Path]}
	}
	sent := make(chan struct{})
	finished := make(chan struct{})
	ct.client = func() error {
		bodies := make(chan *http.Response, len(steps))
		errc := make(chan error, len(steps))
		for i, s := range steps {
			if i == finishAfter {
				for j := 0; j < 2; j++ {
					res := <-bodies
					if _, err := io.ReadAll(res.Body); err != nil {
						return err
					}
				}
				close(finished)
			}
			go func(path string) {
				req, _ := http.NewRequest("GET", "https://dummy.tld"+path, nil)
				res, err := ct.tr.RoundTrip(req)
				if err != nil {
					errc <- err
					return
				}
				bodies <- res
			}(s.path)
			select {
			case <-sent:
			case err := <-errc:
				return err
			}
		}
		return nil
	}
	ct.server = func() error {
		ct.greet()
		var buf bytes.Buffer
		enc := hpack.NewEncoder(&buf)
		respond := func(streamID uint32) error {
			buf.Reset()
			enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
			if err := ct.fr.WriteHeaders(HeadersFrameParam{StreamID: streamID, BlockFragment: buf.Bytes(), EndHeaders: true}); err != nil {
				return err
			}
			return ct.fr.WriteData(streamID, true, nil)
		}
		for i := 0; i < len(steps); {
			f, err := ct.fr.ReadFrame()
			if err != nil {
				return err
			}
			hf, ok := f.(*HeadersFrame)
			if !ok {
				continue
			}
			want := PriorityParam{StreamDep: steps[i].dep, Exclusive: true, Weight: weights[steps[i].path]}
			if hf.Priority != want {
				return fmt.Errorf("%s (stream %d): HEADERS priority = %+v; want %+v", steps[i].path, hf.StreamID, hf.Priority, want)
			}
			i++
			if i == finishAfter {
				for _, id := range []uint32{1, 5} {
					if err := respond(id); err != nil {
						return err
					}
				}
			}
			sent <- struct{}{}
			if i == finishAfter {
				<-finished
			}
		}
		return nil
	}
	ct.run()
}
