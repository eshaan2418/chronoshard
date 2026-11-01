package storage

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/eshaan2418/chronoshard/labels"
)

func ls(kv ...string) labels.Labels { return labels.FromStrings(kv...) }

func eq(n, v string) *labels.Matcher { return labels.MustMatcher(labels.MatchEqual, n, v) }

func fill(t *testing.T, h *Head) {
	t.Helper()
	app := h.Appender()
	for i := int64(0); i < 300; i++ {
		app.Add(ls("__name__", "up", "job", "api", "instance", "a"), i*1000, float64(i))
		app.Add(ls("__name__", "up", "job", "api", "instance", "b"), i*1000, float64(i*2))
		app.Add(ls("__name__", "up", "job", "db"), i*1000, 1)
		app.Add(ls("__name__", "errors_total", "job", "api", "code", "500"), i*1000, float64(i/10))
	}
	if n, err := app.Commit(); err != nil || n != 1200 {
		t.Fatalf("commit: %d %v", n, err)
	}
}

func TestSelectMatchers(t *testing.T) {
	h := NewHead()
	fill(t, h)
	cases := []struct {
		ms   []*labels.Matcher
		want int
	}{
		{[]*labels.Matcher{eq("__name__", "up")}, 3},
		{[]*labels.Matcher{eq("__name__", "up"), eq("job", "api")}, 2},
		{[]*labels.Matcher{eq("__name__", "up"), labels.MustMatcher(labels.MatchNotEqual, "job", "api")}, 1},
		{[]*labels.Matcher{labels.MustMatcher(labels.MatchRegexp, "__name__", "up|errors_total")}, 4},
		{[]*labels.Matcher{labels.MustMatcher(labels.MatchRegexp, "instance", "a|b")}, 2},
		{[]*labels.Matcher{eq("__name__", "up"), labels.MustMatcher(labels.MatchNotRegexp, "instance", "a")}, 2},
		// instance="" matches series without the label.
		{[]*labels.Matcher{eq("__name__", "up"), eq("instance", "")}, 1},
		{[]*labels.Matcher{eq("job", "nope")}, 0},
		// Regexes are anchored: "u" does not match "up".
		{[]*labels.Matcher{labels.MustMatcher(labels.MatchRegexp, "__name__", "u")}, 0},
	}
	for i, c := range cases {
		if got := h.Select(c.ms, 0, 1<<62); len(got) != c.want {
			t.Errorf("case %d %v: %d series, want %d", i, c.ms, len(got), c.want)
		}
	}
	got := h.Select([]*labels.Matcher{eq("instance", "b")}, 100_000, 199_000)
	if len(got) != 1 || len(got[0].Samples) != 100 || got[0].Samples[0].V != 200 {
		t.Fatalf("range select: %+v", got)
	}
}

func TestOutOfOrderAndDuplicates(t *testing.T) {
	h := NewHead()
	app := h.Appender()
	l := ls("__name__", "x")
	app.Add(l, 1000, 1)
	app.Add(l, 2000, 2)
	app.Add(l, 1500, 9) // out of order: rejected
	app.Add(l, 2000, 2) // same sample again: accepted as no-op
	app.Add(l, 2000, 3) // conflicting duplicate: rejected
	app.Add(l, 3000, 3)
	n, _ := app.Commit()
	if n != 4 {
		t.Fatalf("applied %d, want 4", n)
	}
	s := h.Select([]*labels.Matcher{eq("__name__", "x")}, 0, 10000)
	if len(s[0].Samples) != 3 || h.Stats().Rejected != 2 {
		t.Fatalf("%+v %+v", s, h.Stats())
	}
}

func TestChunksCutAt120(t *testing.T) {
	h := NewHead()
	app := h.Appender()
	for i := int64(0); i < 1000; i++ {
		app.Add(ls("__name__", "x"), i*15000, float64(i))
	}
	app.Commit()
	st := h.Stats()
	if st.Chunks != 9 || st.Samples != 1000 {
		t.Fatalf("%+v", st)
	}
	if bps := float64(st.ChunkBytes) / float64(st.Samples); bps > 2.5 {
		t.Fatalf("%.2f bytes/sample", bps)
	}
}

func TestTruncate(t *testing.T) {
	h := NewHead()
	app := h.Appender()
	for i := int64(0); i < 600; i++ {
		app.Add(ls("__name__", "live"), i*1000, 1)
		if i < 200 {
			app.Add(ls("__name__", "stale"), i*1000, 1)
		}
	}
	app.Commit()
	dropped := h.Truncate(400_000)
	if dropped != 1 {
		t.Fatalf("dropped %d series", dropped)
	}
	if got := h.Select([]*labels.Matcher{eq("__name__", "stale")}, 0, 1<<62); len(got) != 0 {
		t.Fatal("stale series survived")
	}
	live := h.Select([]*labels.Matcher{eq("__name__", "live")}, 0, 1<<62)
	if len(live) != 1 || live[0].Samples[len(live[0].Samples)-1].T != 599_000 {
		t.Fatal("live series damaged")
	}
	// The index still works for new series after GC.
	app.Add(ls("__name__", "new"), 700_000, 5)
	app.Commit()
	if len(h.Select([]*labels.Matcher{eq("__name__", "new")}, 0, 1<<62)) != 1 {
		t.Fatal("new series not found after truncate")
	}
}

func count(h *Head) (series, samples int) {
	for _, s := range h.Select([]*labels.Matcher{labels.MustMatcher(labels.MatchRegexp, "__name__", ".+")}, -1<<62, 1<<62) {
		series++
		samples += len(s.Samples)
	}
	return
}

func TestDurabilityWALAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	want := map[string]int{}
	write := func(from, to int64) {
		app := h.Appender()
		for i := from; i < to; i++ {
			name := fmt.Sprintf("m%d", rng.Intn(50))
			l := ls("__name__", name, "k", "v")
			app.Add(l, i*1000, rng.Float64())
			want[name]++
			if i%97 == 0 {
				app.Commit()
			}
		}
		app.Commit()
	}
	write(0, 5000)
	h.Close()

	// 1. Reopen from WAL only (Open also checkpoints).
	if h, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	if s, n := count(h); s != 50 || n != 5000 {
		t.Fatalf("after WAL replay: %d series %d samples", s, n)
	}
	// 2. More data on top of the snapshot, then reopen: snapshot + WAL tail.
	write(5000, 8000)
	h.Close()
	if h, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	if s, n := count(h); s != 50 || n != 8000 {
		t.Fatalf("after snapshot+WAL: %d series %d samples", s, n)
	}
	// 3. Appends continue into the reloaded open chunks.
	write(8000, 8100)
	h.Close()
	if h, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	if _, n := count(h); n != 8100 {
		t.Fatalf("after third reopen: %d samples", n)
	}
	for name, c := range want {
		got := h.Select([]*labels.Matcher{eq("__name__", name)}, -1<<62, 1<<62)
		if len(got) != 1 || len(got[0].Samples) != c {
			t.Fatalf("%s: got %v want %d", name, len(got), c)
		}
	}
	h.Close()
}

func TestTornWALTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	h, _ := Open(dir)
	app := h.Appender()
	for i := int64(0); i < 100; i++ {
		app.Add(ls("__name__", "x"), i, 1)
		app.Commit()
	}
	h.Close()
	segs, _ := listSegments(filepath.Join(dir, "wal"))
	last := segmentName(filepath.Join(dir, "wal"), segs[len(segs)-1])
	data, _ := os.ReadFile(last)
	os.WriteFile(last, data[:len(data)-5], 0o644) // tear the final record
	h, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, n := count(h); n != 99 {
		t.Fatalf("%d samples after torn tail, want 99", n)
	}
	h.Close()
}

func TestCorruptSnapshotIsAnError(t *testing.T) {
	dir := t.TempDir()
	h, _ := Open(dir)
	app := h.Appender()
	app.Add(ls("__name__", "x"), 1, 1)
	app.Commit()
	if err := h.Checkpoint(dir); err != nil {
		t.Fatal(err)
	}
	h.Close()
	p := filepath.Join(dir, "snapshot")
	data, _ := os.ReadFile(p)
	data[len(data)/2] ^= 1
	os.WriteFile(p, data, 0o644)
	if _, err := Open(dir); err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
}
