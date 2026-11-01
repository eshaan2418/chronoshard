// Package storage holds series in memory as Gorilla chunks, indexes them by
// label, and makes them durable with a write-ahead log plus periodic
// snapshots.
package storage

import (
	"errors"
	"sort"
	"sync"

	"github.com/eshaan2418/chronoshard/chunk"
	"github.com/eshaan2418/chronoshard/labels"
)

// SamplesPerChunk follows Prometheus: about 30 minutes at a 15 s interval.
const SamplesPerChunk = 120

var (
	ErrOutOfOrder = errors.New("storage: out-of-order sample")
	ErrDuplicate  = errors.New("storage: duplicate timestamp with a different value")
)

type Sample struct {
	T int64
	V float64
}

// Series is a query result: one label set and its samples in range.
type Series struct {
	Labels  labels.Labels
	Samples []Sample
}

type memSeries struct {
	id     uint32
	lset   labels.Labels
	mu     sync.Mutex
	chunks []*chunk.Chunk // last one is open for appends
	lastT  int64
	lastV  float64
	empty  bool
}

func (s *memSeries) append(t int64, v float64) error {
	if !s.empty {
		if t < s.lastT {
			return ErrOutOfOrder
		}
		if t == s.lastT {
			if v == s.lastV {
				return nil // idempotent re-delivery
			}
			return ErrDuplicate
		}
	}
	if len(s.chunks) == 0 || s.chunks[len(s.chunks)-1].NumSamples() >= SamplesPerChunk {
		s.chunks = append(s.chunks, &chunk.Chunk{})
	}
	if err := s.chunks[len(s.chunks)-1].Append(t, v); err != nil {
		return err
	}
	s.lastT, s.lastV, s.empty = t, v, false
	return nil
}

// Head is the in-memory database.
type Head struct {
	mu       sync.RWMutex
	byHash   map[uint64][]*memSeries
	byID     []*memSeries // index = series id; nil after garbage collection
	postings map[string]map[string][]uint32

	wal *WAL // nil for a purely in-memory head

	statsMu  sync.Mutex
	appended uint64
	rejected uint64
}

func NewHead() *Head {
	return &Head{byHash: map[uint64][]*memSeries{}, postings: map[string]map[string][]uint32{}}
}

func (h *Head) getOrCreate(lset labels.Labels) (s *memSeries, created bool) {
	hash := lset.Hash()
	h.mu.RLock()
	for _, c := range h.byHash[hash] {
		if c.lset.Equal(lset) {
			h.mu.RUnlock()
			return c, false
		}
	}
	h.mu.RUnlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.byHash[hash] {
		if c.lset.Equal(lset) {
			return c, false
		}
	}
	s = &memSeries{id: uint32(len(h.byID)), lset: lset, empty: true}
	h.byID = append(h.byID, s)
	h.byHash[hash] = append(h.byHash[hash], s)
	for _, l := range lset {
		vals := h.postings[l.Name]
		if vals == nil {
			vals = map[string][]uint32{}
			h.postings[l.Name] = vals
		}
		vals[l.Value] = append(vals[l.Value], s.id) // ids increase, so lists stay sorted
	}
	return s, true
}

// Appender batches samples; Commit logs them to the WAL and applies them.
type Appender struct {
	h       *Head
	series  []*memSeries
	created []*memSeries
	samples []walSample
}

func (h *Head) Appender() *Appender { return &Appender{h: h} }

func (a *Appender) Add(lset labels.Labels, t int64, v float64) {
	s, created := a.h.getOrCreate(lset)
	if created {
		a.created = append(a.created, s)
	}
	a.series = append(a.series, s)
	a.samples = append(a.samples, walSample{ref: s.id, t: t, v: v})
}

// Commit returns the number of samples applied; out-of-order and conflicting
// samples are counted and skipped, as Prometheus does.
func (a *Appender) Commit() (applied int, err error) {
	if a.h.wal != nil {
		if err := a.h.wal.LogSeries(a.created); err != nil {
			return 0, err
		}
		if err := a.h.wal.LogSamples(a.samples); err != nil {
			return 0, err
		}
	}
	rejected := 0
	for i, s := range a.series {
		smp := a.samples[i]
		s.mu.Lock()
		e := s.append(smp.t, smp.v)
		s.mu.Unlock()
		if e != nil {
			rejected++
		}
	}
	a.h.statsMu.Lock()
	a.h.appended += uint64(len(a.series) - rejected)
	a.h.rejected += uint64(rejected)
	a.h.statsMu.Unlock()
	applied = len(a.series) - rejected
	a.series, a.created, a.samples = a.series[:0], a.created[:0], a.samples[:0]
	return applied, nil
}

// postingsFor returns the sorted ids of series matching all matchers.
func (h *Head) postingsFor(ms []*labels.Matcher) []uint32 {
	var result []uint32
	restricted := false
	for _, m := range ms {
		if m.Matches("") {
			continue // matches series lacking the label: cannot narrow via postings
		}
		var ids []uint32
		for v, list := range h.postings[m.Name] {
			if m.Matches(v) {
				ids = union(ids, list)
			}
		}
		if !restricted {
			result, restricted = ids, true
		} else {
			result = intersect(result, ids)
		}
		if len(result) == 0 {
			return nil
		}
	}
	if !restricted {
		result = make([]uint32, 0, len(h.byID))
		for _, s := range h.byID {
			if s != nil {
				result = append(result, s.id)
			}
		}
	}
	// Final check of every matcher, including the empty-matching ones.
	out := result[:0:0]
	for _, id := range result {
		s := h.byID[id]
		if s == nil {
			continue
		}
		ok := true
		for _, m := range ms {
			if !m.Matches(s.lset.Get(m.Name)) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, id)
		}
	}
	return out
}

// Select returns matching series with their samples in [mint, maxt].
func (h *Head) Select(ms []*labels.Matcher, mint, maxt int64) []Series {
	h.mu.RLock()
	ids := h.postingsFor(ms)
	ss := make([]*memSeries, len(ids))
	for i, id := range ids {
		ss[i] = h.byID[id]
	}
	h.mu.RUnlock()
	out := make([]Series, 0, len(ss))
	for _, s := range ss {
		var samples []Sample
		s.mu.Lock()
		for _, c := range s.chunks {
			if c.NumSamples() == 0 || c.MaxTime() < mint || c.MinTime() > maxt {
				continue
			}
			it := c.Iterator()
			for it.Next() {
				t, v := it.At()
				if t > maxt {
					break // samples are time-ordered: nothing later can match
				}
				if t >= mint {
					samples = append(samples, Sample{t, v})
				}
			}
		}
		s.mu.Unlock()
		if len(samples) > 0 {
			out = append(out, Series{Labels: s.lset, Samples: samples})
		}
	}
	// Series come back in creation (postings) order, which is also what
	// Prometheus' PromQL engine sees: it selects with sorting disabled.
	// Order-dependent results such as topk ties therefore match.
	return out
}

// LabelNames returns all label names, sorted.
func (h *Head) LabelNames() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.postings))
	for n := range h.postings {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// LabelValues returns the values of a label, sorted.
func (h *Head) LabelValues(name string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.postings[name]))
	for v := range h.postings[name] {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// Truncate drops chunks that end before mint and series left with no data.
func (h *Head) Truncate(mint int64) (droppedSeries int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range h.byID {
		if s == nil {
			continue
		}
		s.mu.Lock()
		keep := s.chunks[:0]
		for _, c := range s.chunks {
			if c.MaxTime() >= mint {
				keep = append(keep, c)
			}
		}
		s.chunks = keep
		gone := len(s.chunks) == 0
		s.mu.Unlock()
		if gone {
			h.byID[s.id] = nil
			droppedSeries++
		}
	}
	if droppedSeries == 0 {
		return 0
	}
	// Rebuild hash and postings without the dropped series.
	h.byHash = map[uint64][]*memSeries{}
	h.postings = map[string]map[string][]uint32{}
	for _, s := range h.byID {
		if s == nil {
			continue
		}
		h.byHash[s.lset.Hash()] = append(h.byHash[s.lset.Hash()], s)
		for _, l := range s.lset {
			if h.postings[l.Name] == nil {
				h.postings[l.Name] = map[string][]uint32{}
			}
			h.postings[l.Name][l.Value] = append(h.postings[l.Name][l.Value], s.id)
		}
	}
	return droppedSeries
}

// Stats is a point-in-time summary.
type Stats struct {
	Series, Chunks, Samples int
	ChunkBytes              int
	Appended, Rejected      uint64
}

func (h *Head) Stats() Stats {
	h.mu.RLock()
	ss := append([]*memSeries(nil), h.byID...)
	h.mu.RUnlock()
	var st Stats
	for _, s := range ss {
		if s == nil {
			continue
		}
		st.Series++
		s.mu.Lock()
		for _, c := range s.chunks {
			st.Chunks++
			st.Samples += c.NumSamples()
			st.ChunkBytes += len(c.Bytes())
		}
		s.mu.Unlock()
	}
	h.statsMu.Lock()
	st.Appended, st.Rejected = h.appended, h.rejected
	h.statsMu.Unlock()
	return st
}

func union(a, b []uint32) []uint32 {
	out := make([]uint32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

func intersect(a, b []uint32) []uint32 {
	out := make([]uint32, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}
