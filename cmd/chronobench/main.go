// Command chronobench measures ingestion and query performance.
//
//	chronobench ingest -series 10000 -samples 480         # in-process, with and without the WAL
//	chronobench query -prom URL -ours URL -series 2000    # same data in both, HTTP query latency
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/eshaan2418/chronoshard/api"
	"github.com/eshaan2418/chronoshard/labels"
	"github.com/eshaan2418/chronoshard/storage"
)

// workload: node-exporter-like series. 70% counters (monotonic, integer
// increments), 30% gauges (2-decimal random walks), 15 s scrape interval
// with small jitter, 50 jobs x N/50 instances.
func workload(nSeries, nSamples int, end int64, seed int64) []api.TimeSeries {
	rng := rand.New(rand.NewSource(seed))
	start := end - int64(nSamples-1)*15000
	out := make([]api.TimeSeries, 0, nSeries)
	for i := 0; i < nSeries; i++ {
		isCounter := i%10 < 7
		name := "node_cpu_seconds_total"
		if !isCounter {
			name = "node_memory_free_ratio"
		}
		l := labels.FromStrings("__name__", name, "job", "job"+strconv.Itoa(i%50), "instance",
			"host"+strconv.Itoa(i/50)+":9100", "series", strconv.Itoa(i))
		ts := api.TimeSeries{Labels: l}
		v := rng.Float64() * 1000
		for j := 0; j < nSamples; j++ {
			t := start + int64(j)*15000
			if rng.Intn(20) == 0 {
				t += rng.Int63n(20) - 10
			}
			if isCounter {
				v += float64(rng.Intn(100))
			} else {
				v = math.Round((v+rng.NormFloat64())*100) / 100
			}
			ts.Samples = append(ts.Samples, storage.Sample{T: t, V: v})
		}
		out = append(out, ts)
	}
	return out
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func ingest(args []string) {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	nSeries := fs.Int("series", 10000, "series")
	nSamples := fs.Int("samples", 480, "samples per series (480 = 2 h at 15 s)")
	runs := fs.Int("runs", 5, "repetitions")
	workers := fs.Int("workers", 4, "concurrent appenders")
	_ = fs.Parse(args) // ExitOnError: never returns an error
	data := workload(*nSeries, *nSamples, time.Now().UnixMilli(), 1)
	total := *nSeries * *nSamples
	result := map[string]any{"series": *nSeries, "samples_per_series": *nSamples, "workers": *workers, "runs": *runs}
	for _, mode := range []string{"memory", "wal"} {
		var rates []float64
		var bps float64
		for r := 0; r < *runs; r++ {
			var h *storage.Head
			dir := ""
			if mode == "wal" {
				dir, _ = os.MkdirTemp("", "chronobench-")
				var err error
				if h, err = storage.Open(dir); err != nil {
					panic(err)
				}
			} else {
				h = storage.NewHead()
			}
			start := time.Now()
			// Scrape-like: each worker owns a slice of series and appends one
			// sample per series per round, committing every round.
			var wg sync.WaitGroup
			per := (*nSeries + *workers - 1) / *workers
			for w := 0; w < *workers; w++ {
				lo, hi := w*per, min((w+1)*per, *nSeries)
				wg.Add(1)
				go func() {
					defer wg.Done()
					app := h.Appender()
					for j := 0; j < *nSamples; j++ {
						for i := lo; i < hi; i++ {
							s := data[i].Samples[j]
							app.Add(data[i].Labels, s.T, s.V)
						}
						if _, err := app.Commit(); err != nil {
							panic(err)
						}
					}
				}()
			}
			wg.Wait()
			el := time.Since(start).Seconds()
			rates = append(rates, float64(total)/el)
			st := h.Stats()
			bps = float64(st.ChunkBytes) / float64(st.Samples)
			if st.Samples != total {
				panic(fmt.Sprintf("stored %d of %d samples", st.Samples, total))
			}
			if err := h.Close(); err != nil {
				panic(err)
			}
			if dir != "" {
				os.RemoveAll(dir)
			}
		}
		result[mode+"_samples_per_sec_median"] = median(rates)
		result[mode+"_samples_per_sec_runs"] = rates
		result["bytes_per_sample"] = bps
		fmt.Printf("%-6s %12.0f samples/s (median of %d), %.3f bytes/sample\n", mode, median(rates), *runs, bps)
	}
	b, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile("bench/results/ingest.json", b, 0o644); err != nil {
		panic(err)
	}
}

func push(target string, data []api.TimeSeries) error {
	n := len(data[0].Samples)
	for j := 0; j < n; j += 4 { // 1-minute batches
		var batch []api.TimeSeries
		for _, s := range data {
			batch = append(batch, api.TimeSeries{Labels: s.Labels, Samples: s.Samples[j:min(j+4, n)]})
		}
		resp, err := http.Post(target+"/api/v1/write", "application/x-protobuf", bytes.NewReader(api.EncodeWriteRequest(batch)))
		if err != nil {
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("%s: %s", target, resp.Status)
		}
	}
	return nil
}

func query(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	prom := fs.String("prom", "http://127.0.0.1:19090", "Prometheus")
	ours := fs.String("ours", "http://127.0.0.1:19201", "chronoshard")
	nSeries := fs.Int("series", 2000, "series")
	nSamples := fs.Int("samples", 240, "samples per series")
	reps := fs.Int("reps", 20, "repetitions per query")
	_ = fs.Parse(args) // ExitOnError: never returns an error
	end := time.Now().Add(-time.Minute).UnixMilli() / 1000 * 1000
	data := workload(*nSeries, *nSamples, end, 2)
	for _, t := range []string{*prom, *ours} {
		if err := push(t, data); err != nil {
			panic(err)
		}
	}
	time.Sleep(2 * time.Second)
	start := end - int64(*nSamples-1)*15000
	qs := []struct{ name, q string }{
		{"raw selector, 1 series", `node_memory_free_ratio{series="7"}`},
		{"sum(rate) over all counters", `sum(rate(node_cpu_seconds_total[5m]))`},
		{"sum by job (rate)", `sum by (job) (rate(node_cpu_seconds_total[5m]))`},
		{"topk(5, avg_over_time)", `topk(5, avg_over_time(node_memory_free_ratio[10m]))`},
		{"regex selector + max", `max(node_memory_free_ratio{job=~"job1.*"})`},
	}
	type row struct {
		Name   string  `json:"name"`
		Query  string  `json:"query"`
		PromMS float64 `json:"prometheus_median_ms"`
		OursMS float64 `json:"chronoshard_median_ms"`
		Series int     `json:"result_series"`
		Kind   string  `json:"kind"`
	}
	var rows []row
	time1 := func(base, path string, p url.Values) (float64, int) {
		t0 := time.Now()
		resp, err := http.Get(base + path + "?" + p.Encode())
		if err != nil {
			panic(err)
		}
		var r struct {
			Data struct {
				Result []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			panic(err)
		}
		resp.Body.Close()
		return float64(time.Since(t0).Microseconds()) / 1000, len(r.Data.Result)
	}
	for _, q := range qs {
		for _, kind := range []string{"instant", "range 1h step 30s"} {
			path, p := "/api/v1/query", url.Values{"query": {q.q}, "time": {fmt.Sprint(end / 1000)}}
			if kind != "instant" {
				path = "/api/v1/query_range"
				p = url.Values{"query": {q.q}, "start": {fmt.Sprint(start/1000 + 600)}, "end": {fmt.Sprint(end / 1000)}, "step": {"30"}}
			}
			var pt, ot []float64
			var n int
			for i := 0; i < *reps+2; i++ { // two warm-up rounds
				a, _ := time1(*prom, path, p)
				b, c := time1(*ours, path, p)
				if i >= 2 {
					pt, ot, n = append(pt, a), append(ot, b), c
				}
			}
			r := row{q.name, q.q, median(pt), median(ot), n, kind}
			rows = append(rows, r)
			fmt.Printf("%-30s %-18s prometheus %8.2f ms  chronoshard %8.2f ms  (%d series)\n", q.name, kind, r.PromMS, r.OursMS, n)
		}
	}
	b, _ := json.MarshalIndent(map[string]any{"series": *nSeries, "samples_per_series": *nSamples, "reps": *reps, "rows": rows}, "", "  ")
	if err := os.WriteFile("bench/results/query.json", b, 0o644); err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: chronobench ingest|query [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "ingest":
		ingest(os.Args[2:])
	case "query":
		query(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown mode", os.Args[1])
		os.Exit(2)
	}
}
