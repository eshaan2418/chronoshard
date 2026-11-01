// Package api serves the subset of the Prometheus HTTP API that Grafana and
// promtool need, plus Prometheus remote write for ingestion.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/eshaan2418/chronoshard/labels"
	"github.com/eshaan2418/chronoshard/promql"
	"github.com/eshaan2418/chronoshard/storage"
)

type Handler struct {
	Head *storage.Head
	mux  *http.ServeMux
}

func New(h *storage.Head) *Handler {
	a := &Handler{Head: h, mux: http.NewServeMux()}
	a.mux.HandleFunc("/api/v1/query", a.query)
	a.mux.HandleFunc("/api/v1/query_range", a.queryRange)
	a.mux.HandleFunc("/api/v1/series", a.series)
	a.mux.HandleFunc("/api/v1/labels", a.labelNames)
	a.mux.HandleFunc("/api/v1/label/", a.labelValues)
	a.mux.HandleFunc("/api/v1/write", a.remoteWrite)
	a.mux.HandleFunc("/api/v1/status/tsdb", a.status)
	a.mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "OK\n") })
	return a
}

func (a *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { a.mux.ServeHTTP(w, r) }

type response struct {
	Status    string `json:"status"`
	Data      any    `json:"data,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, typ string, err error) {
	code := http.StatusBadRequest
	if typ == "execution" {
		code = http.StatusUnprocessableEntity
	}
	writeJSON(w, code, response{Status: "error", ErrorType: typ, Error: err.Error()})
}

// parseTime accepts RFC 3339 or Unix seconds with optional fraction.
func parseTime(s string, def time.Time) (int64, error) {
	if s == "" {
		return def.UnixMilli(), nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(math.Round(f * 1000)), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %q as a timestamp", s)
	}
	return t.UnixMilli(), nil
}

func parseStep(s string) (time.Duration, error) {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}
	return promql.ParseDuration(s)
}

func formatValue(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func pair(t int64, v float64) [2]any { return [2]any{float64(t) / 1000, formatValue(v)} }

func metricJSON(l labels.Labels) map[string]string { return l.Map() }

func (a *Handler) query(w http.ResponseWriter, r *http.Request) {
	ts, err := parseTime(r.FormValue("time"), time.Now())
	if err != nil {
		fail(w, "bad_data", err)
		return
	}
	v, err := promql.InstantQuery(a.Head, r.FormValue("query"), ts)
	if err != nil {
		fail(w, "bad_data", err)
		return
	}
	switch x := v.(type) {
	case promql.Scalar:
		writeJSON(w, 200, response{Status: "success", Data: map[string]any{"resultType": "scalar", "result": pair(x.T, x.V)}})
	case promql.Vector:
		res := make([]map[string]any, 0, len(x))
		for _, s := range x {
			res = append(res, map[string]any{"metric": metricJSON(s.Metric), "value": pair(s.T, s.V)})
		}
		writeJSON(w, 200, response{Status: "success", Data: map[string]any{"resultType": "vector", "result": res}})
	case promql.Matrix:
		writeJSON(w, 200, response{Status: "success", Data: map[string]any{"resultType": "matrix", "result": matrixJSON(x)}})
	}
}

func matrixJSON(m promql.Matrix) []map[string]any {
	res := make([]map[string]any, 0, len(m))
	for _, s := range m {
		vals := make([][2]any, len(s.Points))
		for i, p := range s.Points {
			vals[i] = pair(p.T, p.V)
		}
		res = append(res, map[string]any{"metric": metricJSON(s.Metric), "values": vals})
	}
	return res
}

func (a *Handler) queryRange(w http.ResponseWriter, r *http.Request) {
	start, err1 := parseTime(r.FormValue("start"), time.Time{})
	end, err2 := parseTime(r.FormValue("end"), time.Time{})
	step, err3 := parseStep(r.FormValue("step"))
	if err := errors.Join(err1, err2, err3); err != nil {
		fail(w, "bad_data", err)
		return
	}
	m, err := promql.RangeQuery(a.Head, r.FormValue("query"), start, end, step)
	if err != nil {
		fail(w, "bad_data", err)
		return
	}
	writeJSON(w, 200, response{Status: "success", Data: map[string]any{"resultType": "matrix", "result": matrixJSON(m)}})
}

func (a *Handler) selectors(r *http.Request) ([][]*labels.Matcher, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	var out [][]*labels.Matcher
	for _, s := range r.Form["match[]"] {
		e, err := promql.Parse(s)
		if err != nil {
			return nil, err
		}
		vs, ok := e.(*promql.VectorSelector)
		if !ok {
			return nil, fmt.Errorf("match[] must be a series selector: %q", s)
		}
		out = append(out, vs.Matchers)
	}
	return out, nil
}

func (a *Handler) series(w http.ResponseWriter, r *http.Request) {
	sels, err := a.selectors(r)
	if err != nil || len(sels) == 0 {
		fail(w, "bad_data", errors.Join(err, errors.New("at least one match[] is required")))
		return
	}
	seen := map[uint64]bool{}
	var res []map[string]string
	for _, ms := range sels {
		for _, s := range a.Head.Select(ms, math.MinInt64/2, math.MaxInt64/2) {
			if h := s.Labels.Hash(); !seen[h] {
				seen[h] = true
				res = append(res, s.Labels.Map())
			}
		}
	}
	writeJSON(w, 200, response{Status: "success", Data: res})
}

func (a *Handler) labelNames(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, response{Status: "success", Data: a.Head.LabelNames()})
}

func (a *Handler) labelValues(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[len("/api/v1/label/"):]
	if n := len(name) - len("/values"); n > 0 && name[n:] == "/values" {
		name = name[:n]
	}
	vals := a.Head.LabelValues(name)
	sort.Strings(vals)
	writeJSON(w, 200, response{Status: "success", Data: vals})
}

func (a *Handler) status(w http.ResponseWriter, _ *http.Request) {
	st := a.Head.Stats()
	bps := 0.0
	if st.Samples > 0 {
		bps = float64(st.ChunkBytes) / float64(st.Samples)
	}
	writeJSON(w, 200, response{Status: "success", Data: map[string]any{
		"numSeries": st.Series, "chunks": st.Chunks, "samples": st.Samples, "chunkBytes": st.ChunkBytes,
		"bytesPerSample": bps, "appended": st.Appended, "rejected": st.Rejected,
	}})
}

func (a *Handler) remoteWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	series, err := DecodeWriteRequest(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	app := a.Head.Appender()
	for _, ts := range series {
		for _, s := range ts.Samples {
			app.Add(ts.Labels, s.T, s.V)
		}
	}
	if _, err := app.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
