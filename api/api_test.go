package api

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/eshaan2418/chronoshard/labels"
	"github.com/eshaan2418/chronoshard/storage"
)

func TestRemoteWriteRoundTrip(t *testing.T) {
	in := []TimeSeries{
		{Labels: labels.FromStrings("__name__", "a", "x", "1"), Samples: []storage.Sample{{T: 1000, V: 1.5}, {T: 2000, V: math.Inf(1)}}},
		{Labels: labels.FromStrings("__name__", "b"), Samples: []storage.Sample{{T: -5, V: -2}}},
	}
	out, err := DecodeWriteRequest(EncodeWriteRequest(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || !out[0].Labels.Equal(in[0].Labels) || out[0].Samples[1].V != math.Inf(1) || out[1].Samples[0].T != -5 {
		t.Fatalf("%+v", out)
	}
	if _, err := DecodeWriteRequest([]byte("not snappy")); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestWriteThenQueryOverHTTP(t *testing.T) {
	h := storage.NewHead()
	srv := httptest.NewServer(New(h))
	defer srv.Close()
	var samples []storage.Sample
	for i := int64(0); i <= 20; i++ {
		samples = append(samples, storage.Sample{T: i * 15000, V: float64(i * 15)})
	}
	body := EncodeWriteRequest([]TimeSeries{{Labels: labels.FromStrings("__name__", "req_total", "job", "api"), Samples: samples}})
	resp, err := http.Post(srv.URL+"/api/v1/write", "application/x-protobuf", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("write: %v %v", err, resp.Status)
	}
	get := func(path string, v url.Values) map[string]any {
		r, err := http.Get(srv.URL + path + "?" + v.Encode())
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		var m map[string]any
		json.NewDecoder(r.Body).Decode(&m)
		return m
	}
	m := get("/api/v1/query", url.Values{"query": {"rate(req_total[1m])"}, "time": {"300"}})
	res := m["data"].(map[string]any)["result"].([]any)
	if len(res) != 1 {
		t.Fatalf("instant: %v", m)
	}
	// 15 per 15 s = 1/s; extrapolation arithmetic leaves float rounding.
	if v, _ := strconv.ParseFloat(res[0].(map[string]any)["value"].([]any)[1].(string), 64); math.Abs(v-1) > 1e-9 {
		t.Fatalf("rate = %v", v)
	}
	m = get("/api/v1/query_range", url.Values{"query": {"req_total"}, "start": {"0"}, "end": {"300"}, "step": {"60"}})
	if vals := m["data"].(map[string]any)["result"].([]any)[0].(map[string]any)["values"].([]any); len(vals) != 6 {
		t.Fatalf("range: %v", vals)
	}
	if m = get("/api/v1/query", url.Values{"query": {"sum("}}); m["status"] != "error" {
		t.Fatalf("bad query accepted: %v", m)
	}
	if m = get("/api/v1/labels", nil); len(m["data"].([]any)) != 2 {
		t.Fatalf("labels: %v", m)
	}
	if m = get("/api/v1/series", url.Values{"match[]": {`{job="api"}`}}); len(m["data"].([]any)) != 1 {
		t.Fatalf("series: %v", m)
	}
}
