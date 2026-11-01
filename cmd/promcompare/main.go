// Command promcompare checks chronoshard's PromQL results against a real
// Prometheus. It generates a synthetic workload (counters with resets,
// gauges, irregular scrape jitter, gaps and churn), pushes identical samples
// to both servers via remote write, runs every query in the suite as an
// instant query at several times and as a range query, and diffs the
// results (series matched by labels; values within 1e-9 relative, or both
// NaN).
//
//	prometheus --web.enable-remote-write-receiver --storage.tsdb.path /tmp/p &
//	chronoshard -data /tmp/c -listen :9201 &
//	promcompare -prom http://localhost:9090 -ours http://localhost:9201
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/eshaan2418/chronoshard/api"
	"github.com/eshaan2418/chronoshard/labels"
	"github.com/eshaan2418/chronoshard/storage"
)

var queries = []string{
	`up`,
	`http_requests_total`,
	`http_requests_total{job="api"}`,
	`http_requests_total{code=~"5.."}`,
	`http_requests_total{code!~"2..",job!="db"}`,
	`rate(http_requests_total[1m])`,
	`rate(http_requests_total[5m])`,
	`irate(http_requests_total[5m])`,
	`increase(http_requests_total[10m])`,
	`delta(temperature_celsius[10m])`,
	`sum(rate(http_requests_total[5m]))`,
	`sum by (job) (rate(http_requests_total[5m]))`,
	`sum without (instance) (rate(http_requests_total[5m]))`,
	`avg by (job) (temperature_celsius)`,
	`max by (code) (http_requests_total)`,
	`min without (job, instance) (temperature_celsius)`,
	`count by (job) (up)`,
	`topk(3, temperature_celsius)`,
	`bottomk(2, rate(http_requests_total[5m]))`,
	`temperature_celsius > 25`,
	`temperature_celsius > bool 25`,
	`temperature_celsius * 9 / 5 + 32`,
	`2 ^ 3 * -temperature_celsius`,
	`rate(http_requests_total{code="500"}[5m]) / ignoring(code) rate(http_requests_total{code="200"}[5m])`,
	`sum by (job, instance) (rate(http_requests_total[5m])) / on(job, instance) group_free_mem_bytes`,
	`http_requests_total offset 5m`,
	`rate(http_requests_total[5m] offset 10m)`,
	`avg_over_time(temperature_celsius[10m])`,
	`max_over_time(temperature_celsius[5m])`,
	`min_over_time(temperature_celsius[5m])`,
	`sum_over_time(temperature_celsius[3m])`,
	`count_over_time(temperature_celsius[5m])`,
	`last_over_time(temperature_celsius[5m])`,
	`clamp_min(temperature_celsius, 20)`,
	`clamp_max(temperature_celsius, 22)`,
	`round(temperature_celsius, 0.5)`,
	`abs(delta(temperature_celsius[5m]))`,
	`up == 1 and on(instance) temperature_celsius > 20`,
	`up or vector(0)`,
	`http_requests_total unless http_requests_total{code="500"}`,
	`time()`,
	`scalar(sum(up)) * 2`,
	`sum(rate(http_requests_total[5m])) by (code) > bool 0.5`,
	`count(count by (job) (http_requests_total))`,
	`sum by (job) (increase(http_requests_total[15m])) / sum by (job) (count_over_time(up[15m]))`,
	`churn_gauge`,
	`rate(churn_counter[5m])`,
}

type sample struct {
	t int64
	v float64
}

// generate builds the workload: 12 counters, 4 gauges, up, a static info
// metric and churning series, scraped every ~15 s with jitter and gaps.
func generate(start, end int64, seed int64) []api.TimeSeries {
	rng := rand.New(rand.NewSource(seed))
	var out []api.TimeSeries
	add := func(l labels.Labels, s []sample) {
		ts := api.TimeSeries{Labels: l}
		for _, x := range s {
			ts.Samples = append(ts.Samples, storage.Sample{T: x.t, V: x.v})
		}
		out = append(out, ts)
	}
	scrapes := func(gapProb float64) []int64 {
		var ts []int64
		for t := start; t <= end; t += 15000 {
			if rng.Float64() < gapProb {
				continue
			}
			ts = append(ts, t+rng.Int63n(800)-400)
		}
		return ts
	}
	for _, job := range []string{"api", "db"} {
		for _, inst := range []string{"a:80", "b:80"} {
			add(labels.FromStrings("__name__", "up", "job", job, "instance", inst), func() (s []sample) {
				for _, t := range scrapes(0.02) {
					s = append(s, sample{t, 1})
				}
				return
			}())
			add(labels.FromStrings("__name__", "group_free_mem_bytes", "job", job, "instance", inst), func() (s []sample) {
				for _, t := range scrapes(0) {
					s = append(s, sample{t, float64(1e9 + rng.Intn(1e8))})
				}
				return
			}())
			for _, code := range []string{"200", "404", "500"} {
				var s []sample
				v := float64(rng.Intn(1000))
				for _, t := range scrapes(0.03) {
					v += float64(rng.Intn(50))
					if rng.Float64() < 0.01 {
						v = float64(rng.Intn(20)) // counter reset
					}
					s = append(s, sample{t, v})
				}
				add(labels.FromStrings("__name__", "http_requests_total", "job", job, "instance", inst, "code", code), s)
			}
		}
	}
	for _, room := range []string{"lab", "office", "server", "attic"} {
		var s []sample
		v := 18 + rng.Float64()*10
		for _, t := range scrapes(0.05) {
			v += rng.NormFloat64() * 0.3
			s = append(s, sample{t, math.Round(v*100) / 100})
		}
		add(labels.FromStrings("__name__", "temperature_celsius", "room", room), s)
	}
	// Series that appear and disappear (staleness by absence).
	for i := 0; i < 6; i++ {
		from := start + rng.Int63n(end-start)
		to := from + rng.Int63n(20*60*1000)
		var g, c []sample
		cv := 0.0
		for t := from; t <= to && t <= end; t += 15000 {
			g = append(g, sample{t, rng.Float64()})
			cv += float64(rng.Intn(10))
			c = append(c, sample{t, cv})
		}
		id := strconv.Itoa(i)
		add(labels.FromStrings("__name__", "churn_gauge", "id", id), g)
		add(labels.FromStrings("__name__", "churn_counter", "id", id), c)
	}
	return out
}

// push sends samples in time order, in 1-minute batches, as a remote-write
// client would.
func push(target string, series []api.TimeSeries, start, end int64) error {
	for lo := start - 1000; lo <= end; lo += 60000 {
		hi := lo + 60000
		var batch []api.TimeSeries
		for _, s := range series {
			var part []storage.Sample
			for _, x := range s.Samples {
				if x.T >= lo && x.T < hi {
					part = append(part, x)
				}
			}
			if len(part) > 0 {
				batch = append(batch, api.TimeSeries{Labels: s.Labels, Samples: part})
			}
		}
		if len(batch) == 0 {
			continue
		}
		req, _ := http.NewRequest(http.MethodPost, target+"/api/v1/write", bytes.NewReader(api.EncodeWriteRequest(batch)))
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("%s: remote write returned %s", target, resp.Status)
		}
	}
	return nil
}

type promResult struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
	Data      struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
}

type series struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
	Values [][2]any          `json:"values"`
}

func get(base, path string, params url.Values) (*promResult, error) {
	resp, err := http.Get(base + path + "?" + params.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var r promResult
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

func parseVal(v any) float64 {
	s, _ := v.(string)
	switch s {
	case "+Inf":
		return math.Inf(1)
	case "-Inf":
		return math.Inf(-1)
	case "NaN":
		return math.NaN()
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func approxEqual(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	}
	if a == b {
		return true
	}
	return math.Abs(a-b) <= 1e-9*math.Max(math.Abs(a), math.Abs(b))
}

func key(m map[string]string) string {
	ks := make([]string, 0, len(m))
	for k, v := range m {
		ks = append(ks, k+"="+v)
	}
	sort.Strings(ks)
	return strings.Join(ks, ",")
}

// diff returns "" if equal, else a description of the first difference.
func diff(p, o *promResult) string {
	if p.Status != o.Status {
		return fmt.Sprintf("status prometheus=%s (%s) ours=%s (%s)", p.Status, p.Error, o.Status, o.Error)
	}
	if p.Status != "success" {
		return ""
	}
	if p.Data.ResultType != o.Data.ResultType {
		return fmt.Sprintf("resultType %s vs %s", p.Data.ResultType, o.Data.ResultType)
	}
	if p.Data.ResultType == "scalar" {
		var a, b [2]any
		if err := errors.Join(json.Unmarshal(p.Data.Result, &a), json.Unmarshal(o.Data.Result, &b)); err != nil {
			return "decode: " + err.Error()
		}
		if !approxEqual(parseVal(a[1]), parseVal(b[1])) {
			return fmt.Sprintf("scalar %v vs %v", a[1], b[1])
		}
		return ""
	}
	var ps, ours []series
	if err := errors.Join(json.Unmarshal(p.Data.Result, &ps), json.Unmarshal(o.Data.Result, &ours)); err != nil {
		return "decode: " + err.Error()
	}
	om := map[string]series{}
	for _, s := range ours {
		om[key(s.Metric)] = s
	}
	if len(ps) != len(ours) {
		return fmt.Sprintf("%d series vs %d", len(ps), len(ours))
	}
	for _, s := range ps {
		t, ok := om[key(s.Metric)]
		if !ok {
			return fmt.Sprintf("missing series %v", s.Metric)
		}
		if s.Values == nil {
			if !approxEqual(parseVal(s.Value[1]), parseVal(t.Value[1])) {
				return fmt.Sprintf("%v: %v vs %v", s.Metric, s.Value[1], t.Value[1])
			}
			continue
		}
		if len(s.Values) != len(t.Values) {
			return fmt.Sprintf("%v: %d points vs %d", s.Metric, len(s.Values), len(t.Values))
		}
		for i := range s.Values {
			if s.Values[i][0] != t.Values[i][0] || !approxEqual(parseVal(s.Values[i][1]), parseVal(t.Values[i][1])) {
				return fmt.Sprintf("%v @%v: %v vs %v", s.Metric, s.Values[i][0], s.Values[i][1], t.Values[i][1])
			}
		}
	}
	return ""
}

func main() {
	prom := flag.String("prom", "http://localhost:9090", "Prometheus base URL")
	ours := flag.String("ours", "http://localhost:9201", "chronoshard base URL")
	minutes := flag.Int("minutes", 50, "length of generated history")
	seed := flag.Int64("seed", 1, "workload seed")
	out := flag.String("out", "", "write a JSON summary here")
	verbose := flag.Bool("v", false, "print every mismatch")
	flag.Parse()

	end := time.Now().Add(-time.Minute).UnixMilli() / 1000 * 1000
	start := end - int64(*minutes)*60*1000
	data := generate(start, end, *seed)
	for _, target := range []string{*prom, *ours} {
		if err := push(target, data, start, end); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	time.Sleep(2 * time.Second) // let Prometheus make the samples queryable

	type result struct {
		Query  string `json:"query"`
		Kind   string `json:"kind"`
		At     string `json:"at"`
		Passed bool   `json:"passed"`
		Detail string `json:"detail,omitempty"`
	}
	var results []result
	pass, total := 0, 0
	check := func(q, kind, at, path string, params url.Values) {
		p, err1 := get(*prom, path, params)
		o, err2 := get(*ours, path, params)
		r := result{Query: q, Kind: kind, At: at}
		switch {
		case err1 != nil || err2 != nil:
			r.Detail = fmt.Sprint(err1, err2)
		default:
			r.Detail = diff(p, o)
			r.Passed = r.Detail == ""
		}
		total++
		if r.Passed {
			pass++
		} else if *verbose {
			fmt.Printf("FAIL %s %s %s: %s\n", kind, at, q, r.Detail)
		}
		results = append(results, r)
	}
	for _, q := range queries {
		for _, off := range []int64{0, 7*60_000 + 3_500, 23*60_000 + 11_000, 41 * 60_000} {
			ts := end - off
			check(q, "instant", strconv.FormatInt(ts, 10), "/api/v1/query", url.Values{"query": {q}, "time": {fmt.Sprintf("%.3f", float64(ts)/1000)}})
		}
		params := url.Values{"query": {q}, "start": {fmt.Sprintf("%.3f", float64(start+10*60_000)/1000)},
			"end": {fmt.Sprintf("%.3f", float64(end)/1000)}, "step": {"30"}}
		check(q, "range", "30s", "/api/v1/query_range", params)
	}
	byQuery := map[string]bool{}
	for _, r := range results {
		if _, ok := byQuery[r.Query]; !ok {
			byQuery[r.Query] = true
		}
		if !r.Passed {
			byQuery[r.Query] = false
		}
	}
	qpass := 0
	for _, ok := range byQuery {
		if ok {
			qpass++
		}
	}
	fmt.Printf("%d/%d comparisons identical (%d/%d queries fully identical), %d series, %d samples pushed\n",
		pass, total, qpass, len(byQuery), len(data), countSamples(data))
	if *out != "" {
		b, _ := json.MarshalIndent(map[string]any{"comparisons": total, "identical": pass, "queries": len(byQuery),
			"queries_identical": qpass, "series": len(data), "samples": countSamples(data), "seed": *seed,
			"minutes": *minutes, "results": results}, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if pass != total {
		os.Exit(1)
	}
}

func countSamples(d []api.TimeSeries) int {
	n := 0
	for _, s := range d {
		n += len(s.Samples)
	}
	return n
}
