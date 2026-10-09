package http2

import (
	"bytes"
	"fmt"
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
