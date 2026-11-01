package chunk

import (
	"math"
	"math/rand"
	"testing"
)

type sample struct {
	t int64
	v float64
}

func roundTrip(t *testing.T, in []sample) *Chunk {
	t.Helper()
	var c Chunk
	for _, s := range in {
		if err := c.Append(s.t, s.v); err != nil {
			t.Fatalf("append %v: %v", s, err)
		}
	}
	it := c.Iterator()
	for i, want := range in {
		if !it.Next() {
			t.Fatalf("iterator ended at %d/%d: %v", i, len(in), it.Err())
		}
		gt, gv := it.At()
		if gt != want.t || math.Float64bits(gv) != math.Float64bits(want.v) {
			t.Fatalf("sample %d: got (%d, %v) want (%d, %v)", i, gt, gv, want.t, want.v)
		}
	}
	if it.Next() || it.Err() != nil {
		t.Fatalf("extra samples or error: %v", it.Err())
	}
	return &c
}

func TestEdgeValues(t *testing.T) {
	roundTrip(t, []sample{{1, 0}})
	roundTrip(t, []sample{{-5, 1}, {0, 1}})
	roundTrip(t, []sample{
		{1000, math.NaN()}, {2000, math.Inf(1)}, {3000, math.Inf(-1)}, {4000, math.Copysign(0, -1)},
		{5000, 0}, {6000, math.MaxFloat64}, {7000, math.SmallestNonzeroFloat64}, {8000, 1},
		{9000, 1}, {10000, 1}, {11000, -1e300},
	})
}

func TestDeltaOfDeltaBuckets(t *testing.T) {
	// Exercise each bucket boundary, positive and negative.
	var in []sample
	ts := int64(1_700_000_000_000)
	delta := int64(15000)
	in = append(in, sample{ts, 1})
	ts += delta
	in = append(in, sample{ts, 2})
	for _, dod := range []int64{0, 1, -1, 8192, -8191, 8193, -8192, 65536, -65535, 65537, 524288, -524287, 524289, 1 << 40, -(1 << 39), 0} {
		delta += dod
		if delta <= 0 {
			delta = 1
		}
		ts += delta
		in = append(in, sample{ts, float64(len(in))})
	}
	roundTrip(t, in)
}

func TestRandomSeries(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 2000; trial++ {
		n := 1 + rng.Intn(240)
		ts := rng.Int63n(1 << 45)
		v := rng.NormFloat64() * 100
		var in []sample
		for i := 0; i < n; i++ {
			switch rng.Intn(4) {
			case 0:
				ts += 15000
			case 1:
				ts += 15000 + rng.Int63n(200) - 100
			case 2:
				ts += 1 + rng.Int63n(1<<30)
			default:
				ts += 1
			}
			switch rng.Intn(5) {
			case 0: // unchanged
			case 1:
				v += rng.NormFloat64()
			case 2:
				v = math.Round(v) + 1 // counters
			case 3:
				v = math.Float64frombits(rng.Uint64()) // arbitrary bits, incl. NaN payloads
			default:
				v *= 1.0001
			}
			in = append(in, sample{ts, v})
		}
		roundTrip(t, in)
	}
}

func TestOutOfOrderRejected(t *testing.T) {
	var c Chunk
	if err := c.Append(100, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.Append(100, 2); err != ErrOutOfOrder {
		t.Fatalf("got %v", err)
	}
	if err := c.Append(200, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.Append(150, 2); err != ErrOutOfOrder {
		t.Fatalf("got %v", err)
	}
	if c.NumSamples() != 2 || c.MinTime() != 100 || c.MaxTime() != 200 {
		t.Fatalf("bad bounds %d %d %d", c.NumSamples(), c.MinTime(), c.MaxTime())
	}
}

func TestTruncatedChunkIsAnError(t *testing.T) {
	var c Chunk
	for i := int64(0); i < 50; i++ {
		_ = c.Append(i*1000+i%7, float64(i)*1.5)
	}
	full := c.Bytes()
	it := &Iterator{r: newReader(full[:len(full)/2], uint64(len(full)/2)*8), n: c.n}
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("truncated chunk decoded without error")
	}
}

// A node-exporter-like gauge scraped every 15 s with small jitter compresses
// to roughly the size the Gorilla paper reports.
func TestCompressionOnRealisticSeries(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	var c Chunk
	ts := int64(1_700_000_000_000)
	v := 0.42
	for i := 0; i < 120; i++ {
		ts += 15000
		if rng.Intn(10) == 0 {
			ts += rng.Int63n(5) - 2
		}
		if rng.Intn(3) == 0 {
			v = math.Round((v+rng.NormFloat64()*0.01)*1000) / 1000
		}
		_ = c.Append(ts, v)
	}
	bps := float64(len(c.Bytes())) / 120
	t.Logf("%.2f bytes/sample (raw: 16)", bps)
	if bps > 3 {
		t.Fatalf("compression unexpectedly poor: %.2f bytes/sample", bps)
	}
}

func BenchmarkAppend(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	vals := make([]float64, 120)
	for i := range vals {
		vals[i] = math.Round(rng.NormFloat64()*1000) / 10
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var c Chunk
		for j, v := range vals {
			_ = c.Append(int64(j)*15000, v)
		}
	}
	b.SetBytes(120 * 16)
}

func BenchmarkIterate(b *testing.B) {
	var c Chunk
	for j := 0; j < 120; j++ {
		_ = c.Append(int64(j)*15000, float64(j%17)*1.25)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := c.Iterator()
		for it.Next() {
		}
	}
	b.SetBytes(120 * 16)
}
