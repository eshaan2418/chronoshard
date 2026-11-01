// Package scrape pulls metrics in the Prometheus text exposition format.
package scrape

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/eshaan2418/chronoshard/labels"
	"github.com/eshaan2418/chronoshard/storage"
)

// ParseText parses the text exposition format (comments, TYPE/HELP lines and
// exemplars are ignored). ts is used when a line has no timestamp.
func ParseText(r io.Reader, ts int64, extra labels.Labels) ([]storage.Series, error) {
	var out []storage.Series
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		if i := strings.Index(line, " # "); i >= 0 {
			line = line[:i] // exemplar
		}
		m := map[string]string{}
		for _, l := range extra {
			m[l.Name] = l.Value
		}
		i := strings.IndexAny(line, "{ ")
		if i < 0 {
			return nil, fmt.Errorf("scrape: malformed line %q", line)
		}
		name, rest := line[:i], line[i:]
		if strings.HasPrefix(rest, "{") {
			end := strings.LastIndexByte(rest, '}')
			if end < 0 {
				return nil, fmt.Errorf("scrape: unclosed labels in %q", line)
			}
			if err := parseLabels(rest[1:end], m); err != nil {
				return nil, err
			}
			rest = rest[end+1:]
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return nil, fmt.Errorf("scrape: missing value in %q", line)
		}
		v, err := parseFloat(fields[0])
		if err != nil {
			return nil, err
		}
		t := ts
		if len(fields) > 1 {
			if t, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
				return nil, err
			}
		}
		m[labels.MetricName] = name
		out = append(out, storage.Series{Labels: labels.FromMap(m), Samples: []storage.Sample{{T: t, V: v}}})
	}
	return out, sc.Err()
}

func parseFloat(s string) (float64, error) {
	switch s {
	case "+Inf", "Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(s, 64)
}

func parseLabels(s string, m map[string]string) error {
	for s = strings.TrimSpace(s); s != ""; s = strings.TrimSpace(s) {
		eq := strings.IndexByte(s, '=')
		if eq < 0 || len(s) < eq+2 || s[eq+1] != '"' {
			return fmt.Errorf("scrape: bad label pair in %q", s)
		}
		name := strings.TrimSpace(s[:eq])
		var b strings.Builder
		i := eq + 2
		for ; i < len(s) && s[i] != '"'; i++ {
			if s[i] == '\\' && i+1 < len(s) {
				i++
				switch s[i] {
				case 'n':
					b.WriteByte('\n')
				default:
					b.WriteByte(s[i])
				}
				continue
			}
			b.WriteByte(s[i])
		}
		if i >= len(s) {
			return fmt.Errorf("scrape: unterminated label value")
		}
		m[name] = b.String()
		s = strings.TrimPrefix(strings.TrimSpace(s[i+1:]), ",")
	}
	return nil
}

// Target is one scrape job.
type Target struct {
	Job      string
	URL      string
	Interval time.Duration
}

// Run scrapes t until ctx is done, adding job, instance and up.
func Run(ctx context.Context, t Target, h *storage.Head, log *slog.Logger) {
	client := &http.Client{Timeout: t.Interval}
	tick := time.NewTicker(t.Interval)
	defer tick.Stop()
	inst := strings.TrimPrefix(strings.TrimPrefix(t.URL, "http://"), "https://")
	if i := strings.IndexByte(inst, '/'); i >= 0 {
		inst = inst[:i]
	}
	extra := labels.FromStrings("job", t.Job, "instance", inst)
	for {
		now := time.Now().UnixMilli()
		up := 1.0
		series, err := fetch(ctx, client, t.URL, now, extra)
		if err != nil {
			up = 0
			log.Warn("scrape failed", "job", t.Job, "url", t.URL, "err", err)
		}
		app := h.Appender()
		for _, s := range series {
			app.Add(s.Labels, s.Samples[0].T, s.Samples[0].V)
		}
		app.Add(labels.FromStrings("__name__", "up", "job", t.Job, "instance", inst), now, up)
		if _, err := app.Commit(); err != nil {
			log.Error("commit failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func fetch(ctx context.Context, c *http.Client, url string, ts int64, extra labels.Labels) ([]storage.Series, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return ParseText(resp.Body, ts, extra)
}
