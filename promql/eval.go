package promql

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/eshaan2418/chronoshard/labels"
	"github.com/eshaan2418/chronoshard/storage"
)

var (
	posInf = math.Inf(1)
	nan    = math.NaN()
)

// StaleNaN is Prometheus' staleness marker bit pattern.
const StaleNaN uint64 = 0x7ff0000000000002

func isStale(v float64) bool { return math.Float64bits(v) == StaleNaN }

// LookbackDelta is how far back an instant selector looks for a sample.
var LookbackDelta = 5 * time.Minute

// Queryable is what the engine needs from storage.
type Queryable interface {
	Select(ms []*labels.Matcher, mint, maxt int64) []storage.Series
}

// Point is a timestamped value. It aliases storage.Sample so range windows
// can be handed to functions as sub-slices of the selected data, without
// copying.
type Point = storage.Sample

// Sample is one element of an instant vector.
type Sample struct {
	Metric labels.Labels
	Point
}

type Vector []Sample

type Scalar struct {
	T int64
	V float64
}

// SeriesResult is one series of a range-query matrix.
type SeriesResult struct {
	Metric labels.Labels
	Points []Point
	// noName is Metric without __name__, computed once per query.
	noName labels.Labels
}

type Matrix []SeriesResult

// Value is a Vector, Scalar or Matrix.
type Value interface{}

type fn struct {
	args int // -1 = variadic
	call func(ev *evaluator, args []Expr, ts int64) (Value, error)
}

var functions map[string]fn

func init() {
	over := func(agg func([]Point) float64) func(ev *evaluator, args []Expr, ts int64) (Value, error) {
		return func(ev *evaluator, args []Expr, ts int64) (Value, error) {
			ms, ok := args[0].(*MatrixSelector)
			if !ok {
				return nil, errors.New("expected a range vector")
			}
			var out Vector
			for _, s := range ev.rangeSamples(ms, ts) {
				if len(s.Points) == 0 {
					continue
				}
				out = append(out, Sample{Metric: s.noName, Point: Point{T: ts, V: agg(s.Points)}})
			}
			return out, nil
		}
	}
	rateFn := func(isCounter, isRate bool) func(ev *evaluator, args []Expr, ts int64) (Value, error) {
		return func(ev *evaluator, args []Expr, ts int64) (Value, error) {
			ms, ok := args[0].(*MatrixSelector)
			if !ok {
				return nil, errors.New("expected a range vector")
			}
			var out Vector
			rangeEnd := ts - ms.VS.Offset.Milliseconds()
			rangeStart := rangeEnd - ms.Range.Milliseconds()
			for _, s := range ev.rangeSamples(ms, ts) {
				if v, ok := extrapolatedRate(s.Points, rangeStart, rangeEnd, isCounter, isRate); ok {
					out = append(out, Sample{Metric: s.noName, Point: Point{T: ts, V: v}})
				}
			}
			return out, nil
		}
	}
	mapFn := func(f func(float64) float64) func(ev *evaluator, args []Expr, ts int64) (Value, error) {
		return func(ev *evaluator, args []Expr, ts int64) (Value, error) {
			v, err := ev.vector(args[0], ts)
			if err != nil {
				return nil, err
			}
			out := make(Vector, len(v))
			for i, s := range v {
				out[i] = Sample{Metric: s.Metric.Without(labels.MetricName), Point: Point{T: ts, V: f(s.V)}}
			}
			return out, nil
		}
	}
	functions = map[string]fn{
		"rate":     {1, rateFn(true, true)},
		"increase": {1, rateFn(true, false)},
		"delta":    {1, rateFn(false, false)},
		"irate":    {1, irate},
		"avg_over_time": {1, over(func(p []Point) float64 {
			var s float64
			for _, x := range p {
				s += x.V
			}
			return s / float64(len(p))
		})},
		"sum_over_time": {1, over(func(p []Point) float64 {
			var s float64
			for _, x := range p {
				s += x.V
			}
			return s
		})},
		"min_over_time": {1, over(func(p []Point) float64 {
			m := p[0].V
			for _, x := range p[1:] {
				if x.V < m || math.IsNaN(m) {
					m = x.V
				}
			}
			return m
		})},
		"max_over_time": {1, over(func(p []Point) float64 {
			m := p[0].V
			for _, x := range p[1:] {
				if x.V > m || math.IsNaN(m) {
					m = x.V
				}
			}
			return m
		})},
		"count_over_time": {1, over(func(p []Point) float64 { return float64(len(p)) })},
		"last_over_time":  {1, nil}, // set below: keeps the metric name
		"abs":             {1, mapFn(math.Abs)},
		"ceil":            {1, mapFn(math.Ceil)},
		"floor":           {1, mapFn(math.Floor)},
		"sqrt":            {1, mapFn(math.Sqrt)},
		"exp":             {1, mapFn(math.Exp)},
		"ln":              {1, mapFn(math.Log)},
		"log2":            {1, mapFn(math.Log2)},
		"log10":           {1, mapFn(math.Log10)},
		"round":           {-1, roundFn},
		"clamp_min":       {2, clampFn(true)},
		"clamp_max":       {2, clampFn(false)},
		"time":            {0, func(_ *evaluator, _ []Expr, ts int64) (Value, error) { return Scalar{ts, float64(ts) / 1000}, nil }},
		"vector":          {1, vectorFn},
		"scalar":          {1, scalarFn},
	}
	functions["last_over_time"] = fn{1, func(ev *evaluator, args []Expr, ts int64) (Value, error) {
		ms, ok := args[0].(*MatrixSelector)
		if !ok {
			return nil, errors.New("expected a range vector")
		}
		var out Vector
		for _, s := range ev.rangeSamples(ms, ts) {
			if len(s.Points) > 0 {
				out = append(out, Sample{Metric: s.Metric, Point: Point{T: ts, V: s.Points[len(s.Points)-1].V}})
			}
		}
		return out, nil
	}}
}

// extrapolatedRate follows Prometheus' algorithm: the increase between the
// first and last sample (corrected for counter resets), extrapolated toward
// the range boundaries unless the series appears to start or end inside it.
func extrapolatedRate(p []Point, rangeStart, rangeEnd int64, isCounter, isRate bool) (float64, bool) {
	if len(p) < 2 {
		return 0, false
	}
	first, last := p[0], p[len(p)-1]
	result := last.V - first.V
	if isCounter {
		prev := first.V
		for _, x := range p[1:] {
			if x.V < prev {
				result += prev
			}
			prev = x.V
		}
	}
	durationToStart := float64(first.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-last.T) / 1000
	sampled := float64(last.T-first.T) / 1000
	avgStep := sampled / float64(len(p)-1)
	// If a boundary is close to a sample, assume the series continues to it;
	// otherwise extrapolate only half an average step (the series probably
	// starts or ends inside the range).
	threshold := avgStep * 1.1
	if durationToStart >= threshold {
		durationToStart = avgStep / 2
	}
	// Counters cannot go below zero: never extrapolate past the point where
	// the line through the samples would hit zero. Applied after the
	// threshold cap, in the same order as Prometheus.
	if isCounter && result > 0 && first.V >= 0 {
		if toZero := sampled * (first.V / result); toZero < durationToStart {
			durationToStart = toZero
		}
	}
	if durationToEnd >= threshold {
		durationToEnd = avgStep / 2
	}
	interval := sampled + durationToStart + durationToEnd
	factor := interval / sampled
	if isRate {
		factor /= float64(rangeEnd-rangeStart) / 1000
	}
	return result * factor, true
}

func irate(ev *evaluator, args []Expr, ts int64) (Value, error) {
	ms, ok := args[0].(*MatrixSelector)
	if !ok {
		return nil, errors.New("expected a range vector")
	}
	var out Vector
	for _, s := range ev.rangeSamples(ms, ts) {
		n := len(s.Points)
		if n < 2 {
			continue
		}
		a, b := s.Points[n-2], s.Points[n-1]
		d := b.V - a.V
		if b.V < a.V {
			d = b.V // counter reset
		}
		dt := float64(b.T-a.T) / 1000
		if dt == 0 {
			continue
		}
		out = append(out, Sample{Metric: s.noName, Point: Point{T: ts, V: d / dt}})
	}
	return out, nil
}

func roundFn(ev *evaluator, args []Expr, ts int64) (Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, errors.New("round expects 1 or 2 arguments")
	}
	to := 1.0
	if len(args) == 2 {
		s, err := ev.scalar(args[1], ts)
		if err != nil {
			return nil, err
		}
		to = s
	}
	v, err := ev.vector(args[0], ts)
	if err != nil {
		return nil, err
	}
	inv := 1 / to
	out := make(Vector, len(v))
	for i, s := range v {
		// Prometheus rounds half up: floor(x*inv + 0.5) / inv.
		out[i] = Sample{Metric: s.Metric.Without(labels.MetricName), Point: Point{T: ts, V: math.Floor(s.V*inv+0.5) / inv}}
	}
	return out, nil
}

func clampFn(isMin bool) func(ev *evaluator, args []Expr, ts int64) (Value, error) {
	return func(ev *evaluator, args []Expr, ts int64) (Value, error) {
		v, err := ev.vector(args[0], ts)
		if err != nil {
			return nil, err
		}
		bound, err := ev.scalar(args[1], ts)
		if err != nil {
			return nil, err
		}
		out := make(Vector, len(v))
		for i, s := range v {
			x := s.V
			if isMin {
				x = math.Max(x, bound)
			} else {
				x = math.Min(x, bound)
			}
			out[i] = Sample{Metric: s.Metric.Without(labels.MetricName), Point: Point{T: ts, V: x}}
		}
		return out, nil
	}
}

func vectorFn(ev *evaluator, args []Expr, ts int64) (Value, error) {
	s, err := ev.scalar(args[0], ts)
	if err != nil {
		return nil, err
	}
	return Vector{{Metric: labels.Labels{}, Point: Point{T: ts, V: s}}}, nil
}

func scalarFn(ev *evaluator, args []Expr, ts int64) (Value, error) {
	v, err := ev.vector(args[0], ts)
	if err != nil {
		return nil, err
	}
	if len(v) != 1 {
		return Scalar{ts, nan}, nil
	}
	return Scalar{ts, v[0].V}, nil
}

// ---------------------------------------------------------------------------
// Evaluator

type evaluator struct {
	q      Queryable
	start  int64
	end    int64
	cache  map[*VectorSelector][]storage.Series
	noName map[*VectorSelector][]labels.Labels
	maxR   map[*VectorSelector]time.Duration
}

// preload finds the widest window each selector needs over the query.
func (ev *evaluator) preload(e Expr) {
	var walk func(Expr, time.Duration)
	walk = func(e Expr, r time.Duration) {
		switch x := e.(type) {
		case *VectorSelector:
			if cur, ok := ev.maxR[x]; !ok || r > cur {
				ev.maxR[x] = r
			}
		case *MatrixSelector:
			walk(x.VS, x.Range)
		case *Call:
			for _, a := range x.Args {
				walk(a, 0)
			}
		case *Aggregate:
			walk(x.Expr, 0)
			if x.Param != nil {
				walk(x.Param, 0)
			}
		case *Binary:
			walk(x.LHS, 0)
			walk(x.RHS, 0)
		case *Paren:
			walk(x.Expr, 0)
		case *Unary:
			walk(x.Expr, 0)
		}
	}
	walk(e, 0)
	for vs, r := range ev.maxR {
		window := r
		if window < LookbackDelta {
			window = LookbackDelta
		}
		mint := ev.start - window.Milliseconds() - vs.Offset.Milliseconds()
		maxt := ev.end - vs.Offset.Milliseconds()
		ss := ev.q.Select(vs.Matchers, mint, maxt)
		ev.cache[vs] = ss
		nn := make([]labels.Labels, len(ss))
		for i, s := range ss {
			nn[i] = s.Labels.Without(labels.MetricName)
		}
		ev.noName[vs] = nn
	}
}

// window returns the samples of s in (lo, hi], excluding stale markers.
func window(s []storage.Sample, lo, hi int64) []storage.Sample {
	i := sort.Search(len(s), func(i int) bool { return s[i].T > lo })
	j := sort.Search(len(s), func(i int) bool { return s[i].T > hi })
	return s[i:j]
}

// instantSelect: the latest sample in (ts-offset-lookback, ts-offset].
func (ev *evaluator) instantSelect(vs *VectorSelector, ts int64) Vector {
	hi := ts - vs.Offset.Milliseconds()
	lo := hi - LookbackDelta.Milliseconds()
	var out Vector
	for _, s := range ev.cache[vs] {
		w := window(s.Samples, lo, hi)
		if len(w) == 0 {
			continue
		}
		last := w[len(w)-1]
		if isStale(last.V) {
			continue
		}
		out = append(out, Sample{Metric: s.Labels, Point: Point{T: ts, V: last.V}})
	}
	return out
}

// rangeSamples: per series, the samples in (ts-offset-range, ts-offset].
// Windows are sub-slices of the preloaded data unless a stale marker has to
// be filtered out.
func (ev *evaluator) rangeSamples(ms *MatrixSelector, ts int64) []SeriesResult {
	hi := ts - ms.VS.Offset.Milliseconds()
	lo := hi - ms.Range.Milliseconds()
	cached := ev.cache[ms.VS]
	names := ev.noName[ms.VS]
	out := make([]SeriesResult, 0, len(cached))
	for i, s := range cached {
		w := window(s.Samples, lo, hi)
		stale := false
		for _, x := range w {
			if isStale(x.V) {
				stale = true
				break
			}
		}
		if stale {
			pts := make([]Point, 0, len(w))
			for _, x := range w {
				if !isStale(x.V) {
					pts = append(pts, x)
				}
			}
			w = pts
		}
		if len(w) > 0 {
			out = append(out, SeriesResult{Metric: s.Labels, Points: w, noName: names[i]})
		}
	}
	return out
}

func (ev *evaluator) eval(e Expr, ts int64) (Value, error) {
	switch x := e.(type) {
	case *NumberLiteral:
		return Scalar{ts, x.Val}, nil
	case *Paren:
		return ev.eval(x.Expr, ts)
	case *VectorSelector:
		return ev.instantSelect(x, ts), nil
	case *MatrixSelector:
		return nil, errors.New("range vectors must be passed to a function")
	case *Call:
		return functions[x.Func].call(ev, x.Args, ts)
	case *Unary:
		v, err := ev.eval(x.Expr, ts)
		if err != nil || x.Op == "+" {
			return v, err
		}
		switch y := v.(type) {
		case Scalar:
			return Scalar{ts, -y.V}, nil
		case Vector:
			out := make(Vector, len(y))
			for i, s := range y {
				out[i] = Sample{Metric: s.Metric.Without(labels.MetricName), Point: Point{T: ts, V: -s.V}}
			}
			return out, nil
		}
	case *Aggregate:
		return ev.aggregate(x, ts)
	case *Binary:
		return ev.binary(x, ts)
	}
	return nil, fmt.Errorf("cannot evaluate %T", e)
}

func (ev *evaluator) vector(e Expr, ts int64) (Vector, error) {
	v, err := ev.eval(e, ts)
	if err != nil {
		return nil, err
	}
	vec, ok := v.(Vector)
	if !ok {
		return nil, fmt.Errorf("expected instant vector, got %T", v)
	}
	return vec, nil
}

func (ev *evaluator) scalar(e Expr, ts int64) (float64, error) {
	v, err := ev.eval(e, ts)
	if err != nil {
		return 0, err
	}
	s, ok := v.(Scalar)
	if !ok {
		return 0, fmt.Errorf("expected scalar, got %T", v)
	}
	return s.V, nil
}

func groupKey(m labels.Labels, a *Aggregate) labels.Labels {
	switch {
	case a.By:
		return m.Keep(a.Labels...)
	case a.Without:
		return m.Without(append([]string{labels.MetricName}, a.Labels...)...)
	default:
		return labels.Labels{}
	}
}

func (ev *evaluator) aggregate(a *Aggregate, ts int64) (Value, error) {
	v, err := ev.vector(a.Expr, ts)
	if err != nil {
		return nil, err
	}
	type group struct {
		labels  labels.Labels
		sum     float64
		count   int
		min     float64
		max     float64
		members Vector
	}
	groups := map[uint64]*group{}
	var order []uint64
	for _, s := range v {
		k := groupKey(s.Metric, a)
		h := k.Hash()
		g := groups[h]
		if g == nil {
			g = &group{labels: k, min: s.V, max: s.V}
			groups[h] = g
			order = append(order, h)
		}
		g.sum += s.V
		g.count++
		if s.V < g.min || math.IsNaN(g.min) {
			g.min = s.V
		}
		if s.V > g.max || math.IsNaN(g.max) {
			g.max = s.V
		}
		g.members = append(g.members, s)
	}
	var out Vector
	switch a.Op {
	case "topk", "bottomk":
		kf, err := ev.scalar(a.Param, ts)
		if err != nil {
			return nil, err
		}
		k := int(kf)
		for _, h := range order {
			out = append(out, selectK(groups[h].members, k, a.Op == "topk")...)
		}
		return out, nil
	}
	for _, h := range order {
		g := groups[h]
		var val float64
		switch a.Op {
		case "sum":
			val = g.sum
		case "avg":
			val = g.sum / float64(g.count)
		case "min":
			val = g.min
		case "max":
			val = g.max
		case "count":
			val = float64(g.count)
		}
		out = append(out, Sample{Metric: g.labels, Point: Point{T: ts, V: val}})
	}
	return out, nil
}

// kHeap mirrors Prometheus' vectorByValueHeap (topk) and
// vectorByReverseValueHeap (bottomk): ties at the k-th place are resolved
// by the same heap operations, so results match exactly.
type kHeap struct {
	s       Vector
	reverse bool
}

func (h *kHeap) Len() int { return len(h.s) }
func (h *kHeap) Less(i, j int) bool {
	if math.IsNaN(h.s[i].V) {
		return true
	}
	if h.reverse {
		return h.s[i].V > h.s[j].V
	}
	return h.s[i].V < h.s[j].V
}
func (h *kHeap) Swap(i, j int) { h.s[i], h.s[j] = h.s[j], h.s[i] }
func (h *kHeap) Push(x any)    { h.s = append(h.s, x.(Sample)) }
func (h *kHeap) Pop() any {
	n := len(h.s)
	x := h.s[n-1]
	h.s = h.s[:n-1]
	return x
}

func selectK(in Vector, k int, top bool) Vector {
	if k <= 0 {
		return nil
	}
	h := &kHeap{reverse: !top}
	for _, s := range in {
		switch {
		case len(h.s) < k:
			heap.Push(h, s)
		case top && (h.s[0].V < s.V || (math.IsNaN(h.s[0].V) && !math.IsNaN(s.V))),
			!top && (h.s[0].V > s.V || (math.IsNaN(h.s[0].V) && !math.IsNaN(s.V))):
			h.s[0] = s
			if k > 1 {
				heap.Fix(h, 0)
			}
		}
	}
	return h.s
}

func isComparison(op string) bool {
	switch op {
	case "==", "!=", "<", "<=", ">", ">=":
		return true
	}
	return false
}

func applyOp(op string, a, b float64) (float64, bool) {
	switch op {
	case "+":
		return a + b, true
	case "-":
		return a - b, true
	case "*":
		return a * b, true
	case "/":
		return a / b, true
	case "%":
		return math.Mod(a, b), true
	case "^":
		return math.Pow(a, b), true
	case "==":
		return a, a == b
	case "!=":
		return a, a != b
	case "<":
		return a, a < b
	case "<=":
		return a, a <= b
	case ">":
		return a, a > b
	case ">=":
		return a, a >= b
	}
	return 0, false
}

func boolVal(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (ev *evaluator) matchKey(m labels.Labels, b *Binary) uint64 {
	switch {
	case b.On:
		return m.Keep(b.Matching...).Hash()
	case b.Ignoring:
		return m.Without(append([]string{labels.MetricName}, b.Matching...)...).Hash()
	default:
		return m.Without(labels.MetricName).Hash()
	}
}

func (ev *evaluator) binary(b *Binary, ts int64) (Value, error) {
	lv, err := ev.eval(b.LHS, ts)
	if err != nil {
		return nil, err
	}
	rv, err := ev.eval(b.RHS, ts)
	if err != nil {
		return nil, err
	}
	ls, lIsScalar := lv.(Scalar)
	rs, rIsScalar := rv.(Scalar)
	switch {
	case lIsScalar && rIsScalar:
		v, keep := applyOp(b.Op, ls.V, rs.V)
		if isComparison(b.Op) {
			if !b.ReturnBool {
				return nil, errors.New("comparisons between scalars must use the bool modifier")
			}
			v = boolVal(keep)
		}
		return Scalar{ts, v}, nil
	case lIsScalar || rIsScalar:
		vec, _ := lv.(Vector)
		if lIsScalar {
			vec, _ = rv.(Vector)
		}
		var out Vector
		for _, s := range vec {
			a, c := s.V, rs.V
			if lIsScalar {
				a, c = ls.V, s.V
			}
			v, keep := applyOp(b.Op, a, c)
			m := s.Metric
			if isComparison(b.Op) {
				if b.ReturnBool {
					v, keep = boolVal(keep), true
					m = m.Without(labels.MetricName)
				} else {
					v = s.V // a filter keeps the vector's own value
				}
				if !keep {
					continue
				}
			} else {
				m = m.Without(labels.MetricName)
			}
			out = append(out, Sample{Metric: m, Point: Point{T: ts, V: v}})
		}
		return out, nil
	}
	lvec, ok1 := lv.(Vector)
	rvec, ok2 := rv.(Vector)
	if !ok1 || !ok2 {
		return nil, errors.New("binary operands must be scalars or instant vectors")
	}
	switch b.Op {
	case "and", "or", "unless":
		rkeys := map[uint64]bool{}
		for _, s := range rvec {
			rkeys[ev.matchKey(s.Metric, b)] = true
		}
		var out Vector
		switch b.Op {
		case "and":
			for _, s := range lvec {
				if rkeys[ev.matchKey(s.Metric, b)] {
					out = append(out, s)
				}
			}
		case "unless":
			for _, s := range lvec {
				if !rkeys[ev.matchKey(s.Metric, b)] {
					out = append(out, s)
				}
			}
		default:
			lkeys := map[uint64]bool{}
			for _, s := range lvec {
				lkeys[ev.matchKey(s.Metric, b)] = true
				out = append(out, s)
			}
			for _, s := range rvec {
				if !lkeys[ev.matchKey(s.Metric, b)] {
					out = append(out, s)
				}
			}
		}
		return out, nil
	}
	rm := map[uint64]Sample{}
	for _, s := range rvec {
		k := ev.matchKey(s.Metric, b)
		if _, dup := rm[k]; dup {
			return nil, errors.New("found duplicate series for the match group on the right-hand side")
		}
		rm[k] = s
	}
	seen := map[uint64]bool{}
	var out Vector
	for _, s := range lvec {
		k := ev.matchKey(s.Metric, b)
		r, ok := rm[k]
		if !ok {
			continue
		}
		if seen[k] {
			return nil, errors.New("found duplicate series for the match group on the left-hand side")
		}
		seen[k] = true
		v, keep := applyOp(b.Op, s.V, r.V)
		// One-to-one result labels: on() keeps only the matching labels,
		// ignoring() removes them (as Prometheus' resultMetric does).
		m := s.Metric
		if b.On {
			m = m.Keep(b.Matching...)
		} else if b.Ignoring {
			m = m.Without(b.Matching...)
		}
		if isComparison(b.Op) {
			if b.ReturnBool {
				v, keep = boolVal(keep), true
				m = m.Without(labels.MetricName)
			}
			if !keep {
				continue
			}
		} else {
			m = m.Without(labels.MetricName)
		}
		out = append(out, Sample{Metric: m, Point: Point{T: ts, V: v}})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Public API

// InstantQuery evaluates q at ts (milliseconds).
func InstantQuery(db Queryable, q string, ts int64) (Value, error) {
	e, err := Parse(q)
	if err != nil {
		return nil, err
	}
	ev := &evaluator{q: db, start: ts, end: ts, cache: map[*VectorSelector][]storage.Series{}, noName: map[*VectorSelector][]labels.Labels{}, maxR: map[*VectorSelector]time.Duration{}}
	ev.preload(e)
	if ms, ok := e.(*MatrixSelector); ok {
		var m Matrix
		for _, s := range ev.rangeSamples(ms, ts) {
			m = append(m, s)
		}
		return m, nil
	}
	return ev.eval(e, ts)
}

// RangeQuery evaluates q at every step from start to end (milliseconds).
func RangeQuery(db Queryable, q string, start, end int64, step time.Duration) (Matrix, error) {
	if step <= 0 {
		return nil, errors.New("step must be positive")
	}
	if (end-start)/step.Milliseconds() > 11000 {
		return nil, errors.New("exceeded maximum resolution of 11,000 points per timeseries")
	}
	e, err := Parse(q)
	if err != nil {
		return nil, err
	}
	if _, ok := e.(*MatrixSelector); ok {
		return nil, errors.New("range queries need an instant-vector or scalar expression")
	}
	ev := &evaluator{q: db, start: start, end: end, cache: map[*VectorSelector][]storage.Series{}, noName: map[*VectorSelector][]labels.Labels{}, maxR: map[*VectorSelector]time.Duration{}}
	ev.preload(e)
	byKey := map[uint64]*SeriesResult{}
	var order []uint64
	for ts := start; ts <= end; ts += step.Milliseconds() {
		v, err := ev.eval(e, ts)
		if err != nil {
			return nil, err
		}
		var vec Vector
		switch x := v.(type) {
		case Vector:
			vec = x
		case Scalar:
			vec = Vector{{Metric: labels.Labels{}, Point: Point{T: ts, V: x.V}}}
		}
		for _, s := range vec {
			h := s.Metric.Hash()
			r := byKey[h]
			if r == nil {
				r = &SeriesResult{Metric: s.Metric}
				byKey[h] = r
				order = append(order, h)
			}
			r.Points = append(r.Points, Point{T: ts, V: s.V})
		}
	}
	out := make(Matrix, 0, len(order))
	for _, h := range order {
		out = append(out, *byKey[h])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Metric.String() < out[j].Metric.String() })
	return out, nil
}
