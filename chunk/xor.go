// Package chunk implements Gorilla-style compressed chunks of
// (timestamp, float64) samples (Pelkonen et al., "Gorilla: A Fast, Scalable,
// In-Memory Time Series Database", VLDB 2015).
//
// Timestamps (milliseconds) are stored as delta-of-deltas in variable-width
// buckets; regular scrape intervals cost a single bit per sample. Values are
// XORed with their predecessor; unchanged values cost one bit, and slowly
// changing ones reuse the previous leading/trailing-zero window.
package chunk

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"
)

// Chunk holds up to ~120 samples (by convention) in increasing time order.
type Chunk struct {
	s       bstream
	n       uint16
	t, tDel int64
	v       uint64
	lead    uint8
	trail   uint8
	window  bool // a leading/trailing window has been written
	minT    int64
	maxT    int64
}

var ErrOutOfOrder = errors.New("chunk: sample is not newer than the last one")

// NumSamples returns the number of samples in the chunk.
func (c *Chunk) NumSamples() int { return int(c.n) }

// MinTime and MaxTime bound the chunk's timestamps.
func (c *Chunk) MinTime() int64 { return c.minT }
func (c *Chunk) MaxTime() int64 { return c.maxT }

// Bytes returns the encoded payload. The bit length is implied by NumSamples
// for the reader, but Encode is the self-describing form.
func (c *Chunk) Bytes() []byte { return c.s.b }

// Append adds a sample. Timestamps must be strictly increasing.
func (c *Chunk) Append(t int64, v float64) error {
	vb := math.Float64bits(v)
	switch c.n {
	case 0:
		var buf [binary.MaxVarintLen64]byte
		for _, b := range buf[:binary.PutVarint(buf[:], t)] {
			c.s.writeByte(b)
		}
		c.s.writeBits(vb, 64)
		c.minT = t
	case 1:
		if t <= c.t {
			return ErrOutOfOrder
		}
		d := t - c.t
		var buf [binary.MaxVarintLen64]byte
		for _, b := range buf[:binary.PutUvarint(buf[:], uint64(d))] {
			c.s.writeByte(b)
		}
		c.writeValue(vb)
		c.tDel = d
	default:
		if t <= c.t {
			return ErrOutOfOrder
		}
		d := t - c.t
		dod := d - c.tDel
		switch {
		case dod == 0:
			c.s.writeBit(false)
		case bitRange(dod, 14):
			c.s.writeBits(0b10, 2)
			c.s.writeBits(uint64(dod), 14)
		case bitRange(dod, 17):
			c.s.writeBits(0b110, 3)
			c.s.writeBits(uint64(dod), 17)
		case bitRange(dod, 20):
			c.s.writeBits(0b1110, 4)
			c.s.writeBits(uint64(dod), 20)
		default:
			c.s.writeBits(0b1111, 4)
			c.s.writeBits(uint64(dod), 64)
		}
		c.writeValue(vb)
		c.tDel = d
	}
	c.t, c.v = t, vb
	c.maxT = t
	c.n++
	return nil
}

// bitRange reports whether x fits in an nbits-wide two's-complement field.
func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}

func (c *Chunk) writeValue(vb uint64) {
	x := vb ^ c.v
	if x == 0 {
		c.s.writeBit(false)
		return
	}
	c.s.writeBit(true)
	lead := uint8(bits.LeadingZeros64(x))
	trail := uint8(bits.TrailingZeros64(x))
	if lead >= 32 {
		lead = 31 // must fit in 5 bits
	}
	if c.window && lead >= c.lead && trail >= c.trail {
		// Reuse the previous window: '0' + meaningful bits.
		c.s.writeBit(false)
		c.s.writeBits(x>>c.trail, 64-int(c.lead)-int(c.trail))
		return
	}
	c.lead, c.trail, c.window = lead, trail, true
	sig := 64 - lead - trail
	c.s.writeBit(true)
	c.s.writeBits(uint64(lead), 5)
	// sig is in [1, 64]; 64 is stored as 0 in 6 bits.
	c.s.writeBits(uint64(sig&63), 6)
	c.s.writeBits(x>>trail, int(sig))
}

// Iterator decodes a chunk's samples in order.
type Iterator struct {
	r       bstreamReader
	n, i    uint16
	t, tDel int64
	v       uint64
	lead    uint8
	trail   uint8
	err     error
}

// Iterator returns a fresh iterator over the chunk's current contents.
func (c *Chunk) Iterator() *Iterator {
	bitsUsed := uint64(len(c.s.b))*8 - uint64(c.s.count)
	return &Iterator{r: newReader(c.s.b, bitsUsed), n: c.n}
}

// Next advances to the next sample, returning false at the end or on error.
func (it *Iterator) Next() bool {
	if it.err != nil || it.i >= it.n {
		return false
	}
	switch it.i {
	case 0:
		t, err := it.readVarint()
		if err != nil {
			return it.fail(err)
		}
		v, err := it.r.readBits(64)
		if err != nil {
			return it.fail(err)
		}
		it.t, it.v = t, v
	case 1:
		d, err := it.readUvarint()
		if err != nil {
			return it.fail(err)
		}
		it.tDel = int64(d)
		it.t += it.tDel
		if err := it.readValue(); err != nil {
			return it.fail(err)
		}
	default:
		var prefix uint8
		for k := 0; k < 4; k++ {
			b, err := it.r.readBit()
			if err != nil {
				return it.fail(err)
			}
			if !b {
				break
			}
			prefix++
		}
		var dod int64
		if prefix > 0 {
			width := [...]int{0, 14, 17, 20, 64}[prefix]
			u, err := it.r.readBits(width)
			if err != nil {
				return it.fail(err)
			}
			dod = signExtend(u, width)
		}
		it.tDel += dod
		it.t += it.tDel
		if err := it.readValue(); err != nil {
			return it.fail(err)
		}
	}
	it.i++
	return true
}

// signExtend decodes a width-bit field written by Append. The encodable range
// is asymmetric, [-(2^(w-1)-1), 2^(w-1)] (as in Prometheus), so only values
// strictly above 2^(w-1) are negative.
func signExtend(u uint64, width int) int64 {
	if width == 64 {
		return int64(u)
	}
	if u > 1<<(width-1) {
		return int64(u) - 1<<width
	}
	return int64(u)
}

func (it *Iterator) readValue() error {
	b, err := it.r.readBit()
	if err != nil {
		return err
	}
	if !b {
		return nil // same value
	}
	newWindow, err := it.r.readBit()
	if err != nil {
		return err
	}
	if newWindow {
		l, err := it.r.readBits(5)
		if err != nil {
			return err
		}
		s, err := it.r.readBits(6)
		if err != nil {
			return err
		}
		if s == 0 {
			s = 64
		}
		it.lead = uint8(l)
		it.trail = uint8(64 - l - s)
	}
	sig := 64 - int(it.lead) - int(it.trail)
	x, err := it.r.readBits(sig)
	if err != nil {
		return err
	}
	it.v ^= x << it.trail
	return nil
}

func (it *Iterator) readUvarint() (uint64, error) {
	var x uint64
	var s uint
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := it.r.readBits(8)
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			return x | b<<s, nil
		}
		x |= (b & 0x7f) << s
		s += 7
	}
	return 0, errors.New("chunk: varint overflow")
}

func (it *Iterator) readVarint() (int64, error) {
	ux, err := it.readUvarint()
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, err
}

func (it *Iterator) fail(err error) bool {
	if errors.Is(err, io.EOF) {
		err = errors.New("chunk: truncated")
	}
	it.err = err
	return false
}

// At returns the current sample.
func (it *Iterator) At() (int64, float64) { return it.t, math.Float64frombits(it.v) }

// Err returns the first decoding error, if any.
func (it *Iterator) Err() error { return it.err }

// FromBytes wraps an encoded chunk payload holding n samples for reading
// (for example from a snapshot). The returned chunk must not be appended to.
func FromBytes(b []byte, n int) (*Chunk, error) {
	if n < 0 || n > math.MaxUint16 {
		return nil, errors.New("chunk: bad sample count")
	}
	c := &Chunk{n: uint16(n)}
	c.s.b = b
	return c, nil
}
