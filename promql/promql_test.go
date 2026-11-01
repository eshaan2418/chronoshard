package promql

import (
	"math"
	"testing"
	"time"

	"github.com/eshaan2418/chronoshard/labels"
	"github.com/eshaan2418/chronoshard/storage"
)

func TestParseRoundTrip(t *testing.T) {
	for _, q := range []string{
		`up`,
		`http_requests_total{job="api",code=~"5.."}`,
		`rate(x[5m] offset 1h)`,
		`sum by (job) (rate(x[1m]))`,
		`sum(rate(x[1m])) without (instance)`,
		`topk(3, x)`,
		`a + b * c ^ d ^ e`,
		`-x > bool 3`,
		`a / ignoring(code) b`,
		`a and on(instance) b`,
		`{__name__=~"up|x"}`,
		`round(x, 0.5)`,
	} {
		if _, err := Parse(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, q := range []string{
		``, `{}`, `{job=""}`, `rate(x)`, `x[5m`, `foo(x)`, `sum(`, `x{a="b"`, `1 +`, `x{a~"b"}`, `x offset`, `(1 + 2`,
		`x{a=~"("}`,
	} {
		if _, err := Parse(q); err == nil {
			t.Errorf("%q parsed without error", q)
		}
	}
}

func TestPrecedence(t *testing.T) {
	db := storage.NewHead()
	check := func(q string, want float64) {
		v, err := InstantQuery(db, q, 0)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if s := v.(Scalar); s.V != want {
			t.Errorf("%s = %v, want %v", q, s.V, want)
		}
	}
	check(`1 + 2 * 3`, 7)
	check(`2 ^ 3 ^ 2`, 512) // right-associative
	check(`-2 ^ 2`, -4)     // unary minus binds looser than ^
	check(`(1 + 2) * 3`, 9)
	check(`10 % 4 - 1`, 1)
	check(`2 > bool 1`, 1)
}

func TestExtrapolatedRate(t *testing.T) {
	// A counter growing 1/s, sampled every 15 s, fully covering a 60 s
	// window: rate is exactly 1 after extrapolation.
	var pts []Point
	for ts := int64(15000); ts <= 60000; ts += 15000 {
		pts = append(pts, Point{T: ts, V: float64(ts / 1000)})
	}
	v, ok := extrapolatedRate(pts, 0, 60000, true, true)
	if !ok || math.Abs(v-1) > 1e-12 {
		t.Fatalf("rate = %v", v)
	}
	// A reset adds the pre-reset value.
	inc, _ := extrapolatedRate([]Point{{T: 0, V: 10}, {T: 10000, V: 20}, {T: 20000, V: 5}, {T: 30000, V: 15}}, 0, 30000, true, false)
	// (20-10) + 5 (counted from zero after the reset) + (15-5) = 25.
	if inc != 25 {
		t.Fatalf("increase across reset = %v, want 25", inc)
	}
}

func TestEndToEndOnHead(t *testing.T) {
	h := storage.NewHead()
	app := h.Appender()
	for i := int64(0); i <= 40; i++ {
		ts := i * 15000
		app.Add(labels.FromStrings("__name__", "req", "job", "a", "code", "200"), ts, float64(i*30))
		app.Add(labels.FromStrings("__name__", "req", "job", "a", "code", "500"), ts, float64(i*3))
		app.Add(labels.FromStrings("__name__", "req", "job", "b", "code", "200"), ts, float64(i*15))
	}
	app.Commit()
	now := int64(600000)
	v, err := InstantQuery(h, `sum by (job) (rate(req[1m]))`, now)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, s := range v.(Vector) {
		got[s.Metric.Get("job")] = s.V
	}
	if math.Abs(got["a"]-2.2) > 1e-9 || math.Abs(got["b"]-1) > 1e-9 {
		t.Fatalf("%v", got)
	}
	v, _ = InstantQuery(h, `req{code="500"} / ignoring(code) req{code="200"}`, now)
	if vec := v.(Vector); len(vec) != 1 || math.Abs(vec[0].V-0.1) > 1e-12 || vec[0].Metric.Get("code") != "" {
		t.Fatalf("ratio: %+v", vec)
	}
	m, err := RangeQuery(h, `sum(req)`, 300000, 600000, 60*time.Second)
	if err != nil || len(m) != 1 || len(m[0].Points) != 6 {
		t.Fatalf("range: %+v %v", m, err)
	}
	// Lookback: a series that stopped 6 min ago is gone.
	if v, _ := InstantQuery(h, `req`, now+6*60*1000); len(v.(Vector)) != 0 {
		t.Fatalf("stale series still returned: %v", v)
	}
	if _, err := InstantQuery(h, `req{job="a"} + on(job) req{job="a"}`, now); err == nil {
		t.Fatal("many-to-many match accepted")
	}
}
