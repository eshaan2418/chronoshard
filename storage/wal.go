package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/eshaan2418/chronoshard/chunk"
	"github.com/eshaan2418/chronoshard/labels"
)

// WAL layout: dir/wal/NNNNNNNN segment files of records
//   [u32 crc32c(type+payload)][u32 len][u8 type][payload]
// type 1 = new series (uvarint id, uvarint n, n x (lenpref name, lenpref value))
// type 2 = samples    (uvarint n, n x (uvarint id, varint t, u64 value bits))
//
// A snapshot (dir/snapshot) captures every series and chunk plus the first
// WAL segment it does not cover; recovery loads it and replays later
// segments. Segments rotate at SegmentSize.

const (
	recSeries  = 1
	recSamples = 2

	SegmentSize = 64 << 20
)

var crcTab = crc32.MakeTable(crc32.Castagnoli)

type walSample struct {
	ref uint32
	t   int64
	v   float64
}

type WAL struct {
	dir  string
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	seg  int
	size int64
	buf  []byte
	// SyncEveryCommit fsyncs after each commit (otherwise data reaches the
	// OS on every commit and the disk on Sync / rotation).
	SyncEveryCommit bool
}

func segmentName(dir string, n int) string { return filepath.Join(dir, fmt.Sprintf("%08d", n)) }

func listSegments(dir string) ([]int, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []int
	for _, e := range ents {
		if n, err := strconv.Atoi(e.Name()); err == nil {
			out = append(out, n)
		}
	}
	sort.Ints(out)
	return out, nil
}

func openWAL(dir string, seg int) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir}
	if err := w.openSegment(seg); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) openSegment(n int) error {
	f, err := os.OpenFile(segmentName(w.dir, n), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, _ := f.Stat()
	w.f, w.w, w.seg, w.size = f, bufio.NewWriterSize(f, 1<<20), n, st.Size()
	return nil
}

func (w *WAL) writeRecord(typ byte, payload []byte) error {
	var hdr [9]byte
	h := crc32.New(crcTab)
	h.Write([]byte{typ})
	h.Write(payload)
	binary.LittleEndian.PutUint32(hdr[0:], h.Sum32())
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(payload)))
	hdr[8] = typ
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(payload); err != nil {
		return err
	}
	w.size += int64(len(payload)) + 9
	return nil
}

func (w *WAL) commit() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.SyncEveryCommit {
		if err := w.f.Sync(); err != nil {
			return err
		}
	}
	if w.size >= SegmentSize {
		return w.rotateLocked()
	}
	return nil
}

func (w *WAL) rotateLocked() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	return w.openSegment(w.seg + 1)
}

// Rotate starts a new segment and returns its number.
func (w *WAL) Rotate() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotateLocked(); err != nil {
		return 0, err
	}
	return w.seg, nil
}

func (w *WAL) LogSeries(ss []*memSeries) error {
	if len(ss) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	b := w.buf[:0]
	for _, s := range ss {
		b = binary.AppendUvarint(b[:0], uint64(s.id))
		b = binary.AppendUvarint(b, uint64(len(s.lset)))
		for _, l := range s.lset {
			b = binary.AppendUvarint(b, uint64(len(l.Name)))
			b = append(b, l.Name...)
			b = binary.AppendUvarint(b, uint64(len(l.Value)))
			b = append(b, l.Value...)
		}
		if err := w.writeRecord(recSeries, b); err != nil {
			return err
		}
	}
	w.buf = b
	return w.commit()
}

func (w *WAL) LogSamples(ss []walSample) error {
	if len(ss) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	b := binary.AppendUvarint(w.buf[:0], uint64(len(ss)))
	for _, s := range ss {
		b = binary.AppendUvarint(b, uint64(s.ref))
		b = binary.AppendVarint(b, s.t)
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(s.v))
	}
	w.buf = b
	if err := w.writeRecord(recSamples, b); err != nil {
		return err
	}
	return w.commit()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// replaySegment applies one segment's records. A torn or corrupt record ends
// the segment (it can only be the unsynced tail written before a crash).
func replaySegment(path string, onSeries func(id uint32, l labels.Labels), onSamples func([]walSample)) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for len(data) >= 9 {
		crc := binary.LittleEndian.Uint32(data)
		n := binary.LittleEndian.Uint32(data[4:])
		typ := data[8]
		if uint64(len(data)-9) < uint64(n) {
			return nil
		}
		payload := data[9 : 9+n]
		h := crc32.New(crcTab)
		h.Write([]byte{typ})
		h.Write(payload)
		if h.Sum32() != crc {
			return nil
		}
		data = data[9+n:]
		switch typ {
		case recSeries:
			id, rest, err := uvarint(payload)
			if err != nil {
				return err
			}
			cnt, rest, err := uvarint(rest)
			if err != nil {
				return err
			}
			ls := make(labels.Labels, 0, cnt)
			for i := uint64(0); i < cnt; i++ {
				var name, val string
				if name, rest, err = lenPrefixed(rest); err != nil {
					return err
				}
				if val, rest, err = lenPrefixed(rest); err != nil {
					return err
				}
				ls = append(ls, labels.Label{Name: name, Value: val})
			}
			onSeries(uint32(id), ls)
		case recSamples:
			cnt, rest, err := uvarint(payload)
			if err != nil {
				return err
			}
			ss := make([]walSample, 0, cnt)
			for i := uint64(0); i < cnt; i++ {
				var id uint64
				if id, rest, err = uvarint(rest); err != nil {
					return err
				}
				t, k := binary.Varint(rest)
				if k <= 0 || len(rest) < k+8 {
					return errors.New("wal: bad sample")
				}
				v := math.Float64frombits(binary.LittleEndian.Uint64(rest[k:]))
				rest = rest[k+8:]
				ss = append(ss, walSample{uint32(id), t, v})
			}
			onSamples(ss)
		default:
			return fmt.Errorf("wal: unknown record type %d", typ)
		}
	}
	return nil
}

func uvarint(b []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, errors.New("wal: bad varint")
	}
	return v, b[n:], nil
}

func lenPrefixed(b []byte) (string, []byte, error) {
	n, rest, err := uvarint(b)
	if err != nil || uint64(len(rest)) < n {
		return "", nil, errors.New("wal: bad string")
	}
	return string(rest[:n]), rest[n:], nil
}

// ---------------------------------------------------------------------------
// Durable head: Open, Checkpoint, Close.

// Open loads dir/snapshot (if any), replays the WAL segments after it, and
// returns a head that logs new data to the WAL.
func Open(dir string) (*Head, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	h := NewHead()
	walDir := filepath.Join(dir, "wal")
	firstSeg, err := h.loadSnapshot(filepath.Join(dir, "snapshot"))
	if err != nil {
		return nil, err
	}
	segs, err := listSegments(walDir)
	if err != nil {
		return nil, err
	}
	// WAL series ids map to head series (ids are stable across restarts
	// because snapshots preserve them and new ids continue from len(byID)).
	byWALID := map[uint32]*memSeries{}
	for _, s := range h.byID {
		if s != nil {
			byWALID[s.id] = s
		}
	}
	last := firstSeg
	for _, n := range segs {
		if n < firstSeg {
			continue
		}
		last = n
		err := replaySegment(segmentName(walDir, n), func(id uint32, l labels.Labels) {
			s, _ := h.getOrCreate(l)
			byWALID[id] = s
		}, func(ss []walSample) {
			for _, smp := range ss {
				if s := byWALID[smp.ref]; s != nil {
					_ = s.append(smp.t, smp.v)
				}
			}
		})
		if err != nil {
			return nil, err
		}
	}
	// Ids assigned during replay must match what the WAL will say next time:
	// re-log the id mapping implicitly by starting a fresh segment after a
	// checkpoint. Simplest correct approach: checkpoint right away.
	w, err := openWAL(walDir, last+1)
	if err != nil {
		return nil, err
	}
	h.wal = w
	if len(segs) > 0 {
		if err := h.Checkpoint(dir); err != nil {
			return nil, err
		}
	}
	return h, nil
}

// Checkpoint writes a snapshot of all data and deletes WAL segments it covers.
func (h *Head) Checkpoint(dir string) error {
	// Hold the head lock while rotating so no sample lands in a segment the
	// snapshot claims to cover without being in the snapshot.
	h.mu.Lock()
	seg, err := h.wal.Rotate()
	if err != nil {
		h.mu.Unlock()
		return err
	}
	type snapSeries struct {
		id     uint32
		lset   labels.Labels
		chunks [][]Sample
	}
	var all []snapSeries
	for _, s := range h.byID {
		if s == nil {
			continue
		}
		s.mu.Lock()
		ss := snapSeries{id: s.id, lset: s.lset}
		for _, c := range s.chunks {
			var samples []Sample
			it := c.Iterator()
			for it.Next() {
				t, v := it.At()
				samples = append(samples, Sample{t, v})
			}
			ss.chunks = append(ss.chunks, samples)
		}
		s.mu.Unlock()
		all = append(all, ss)
	}
	h.mu.Unlock()

	tmp := filepath.Join(dir, "snapshot.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	crc := crc32.New(crcTab)
	mw := &stickyWriter{w: io.MultiWriter(bw, crc)}
	var b []byte
	b = binary.AppendUvarint(b, uint64(seg))
	b = binary.AppendUvarint(b, uint64(len(all)))
	_, _ = mw.Write(b) // sticky: checked once below
	for _, s := range all {
		b = binary.AppendUvarint(b[:0], uint64(s.id))
		b = binary.AppendUvarint(b, uint64(len(s.lset)))
		for _, l := range s.lset {
			b = binary.AppendUvarint(b, uint64(len(l.Name)))
			b = append(b, l.Name...)
			b = binary.AppendUvarint(b, uint64(len(l.Value)))
			b = append(b, l.Value...)
		}
		b = binary.AppendUvarint(b, uint64(len(s.chunks)))
		_, _ = mw.Write(b) // sticky: checked once below
		for _, samples := range s.chunks {
			// Store as a re-encoded Gorilla chunk: compact and self-checking.
			var c chunk.Chunk
			for _, smp := range samples {
				_ = c.Append(smp.T, smp.V)
			}
			b = binary.AppendUvarint(b[:0], uint64(c.NumSamples()))
			b = binary.AppendUvarint(b, uint64(len(c.Bytes())))
			b = append(b, c.Bytes()...)
			_, _ = mw.Write(b) // sticky: checked once below
		}
	}
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], crc.Sum32())
	_, _ = bw.Write(tail[:]) // errors resurface from Flush
	if mw.err != nil {
		f.Close()
		return mw.err
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "snapshot")); err != nil {
		return err
	}
	// Make the rename durable before deleting the WAL it replaces.
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	d.Close()
	if syncErr != nil {
		return syncErr
	}
	segs, _ := listSegments(filepath.Join(dir, "wal"))
	for _, n := range segs {
		if n < seg {
			// Best effort: a leftover old segment is ignored on recovery.
			_ = os.Remove(segmentName(filepath.Join(dir, "wal"), n))
		}
	}
	return nil
}

func (h *Head) loadSnapshot(path string) (firstSeg int, err error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(data) < 4 || crc32.Checksum(data[:len(data)-4], crcTab) != binary.LittleEndian.Uint32(data[len(data)-4:]) {
		return 0, fmt.Errorf("storage: corrupt snapshot %s", path)
	}
	rest := data[:len(data)-4]
	seg, rest, err := uvarint(rest)
	if err != nil {
		return 0, err
	}
	n, rest, err := uvarint(rest)
	if err != nil {
		return 0, err
	}
	for i := uint64(0); i < n; i++ {
		var id, cnt, nchunks uint64
		if id, rest, err = uvarint(rest); err != nil {
			return 0, err
		}
		if cnt, rest, err = uvarint(rest); err != nil {
			return 0, err
		}
		ls := make(labels.Labels, 0, cnt)
		for j := uint64(0); j < cnt; j++ {
			var name, val string
			if name, rest, err = lenPrefixed(rest); err != nil {
				return 0, err
			}
			if val, rest, err = lenPrefixed(rest); err != nil {
				return 0, err
			}
			ls = append(ls, labels.Label{Name: name, Value: val})
		}
		// Preserve ids: pad byID so WAL references after the snapshot resolve.
		for uint64(len(h.byID)) < id {
			h.byID = append(h.byID, nil)
		}
		s, _ := h.getOrCreate(ls)
		if uint64(s.id) != id {
			return 0, fmt.Errorf("storage: snapshot id mismatch (%d != %d)", s.id, id)
		}
		if nchunks, rest, err = uvarint(rest); err != nil {
			return 0, err
		}
		for k := uint64(0); k < nchunks; k++ {
			var ns, nb uint64
			if ns, rest, err = uvarint(rest); err != nil {
				return 0, err
			}
			if nb, rest, err = uvarint(rest); err != nil {
				return 0, err
			}
			if uint64(len(rest)) < nb {
				return 0, errors.New("storage: truncated snapshot chunk")
			}
			c, err := chunk.FromBytes(rest[:nb], int(ns))
			if err != nil {
				return 0, err
			}
			rest = rest[nb:]
			// Re-append so the open chunk can keep growing after restart.
			it := c.Iterator()
			for it.Next() {
				t, v := it.At()
				_ = s.append(t, v)
			}
			if err := it.Err(); err != nil {
				return 0, err
			}
		}
	}
	return int(seg), nil
}

// Close flushes and syncs the WAL.
func (h *Head) Close() error {
	if h.wal == nil {
		return nil
	}
	return h.wal.Close()
}

// stickyWriter remembers the first write error so a long sequence of writes
// can be checked once.
type stickyWriter struct {
	w   io.Writer
	err error
}

func (s *stickyWriter) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	n, err := s.w.Write(p)
	s.err = err
	return n, err
}
