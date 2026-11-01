package chunk

import "io"

// bstream is an append-only bit stream, most significant bit first.
type bstream struct {
	b     []byte
	count uint8 // free bits in the last byte
}

func (s *bstream) writeBit(bit bool) {
	if s.count == 0 {
		s.b = append(s.b, 0)
		s.count = 8
	}
	if bit {
		s.b[len(s.b)-1] |= 1 << (s.count - 1)
	}
	s.count--
}

func (s *bstream) writeByte(v byte) {
	if s.count == 0 {
		s.b = append(s.b, v)
		return
	}
	s.b[len(s.b)-1] |= v >> (8 - s.count)
	s.b = append(s.b, v<<s.count)
}

// writeBits writes the low n bits of u, most significant first.
func (s *bstream) writeBits(u uint64, n int) {
	u <<= 64 - uint(n)
	for n >= 8 {
		s.writeByte(byte(u >> 56))
		u <<= 8
		n -= 8
	}
	for n > 0 {
		s.writeBit(u>>63 == 1)
		u <<= 1
		n--
	}
}

// bstreamReader reads bits from a byte slice it does not own.
type bstreamReader struct {
	b   []byte
	pos uint64 // bit offset
	end uint64 // total readable bits
}

func newReader(b []byte, bits uint64) bstreamReader { return bstreamReader{b: b, end: bits} }

func (r *bstreamReader) readBit() (bool, error) {
	if r.pos >= r.end {
		return false, io.EOF
	}
	bit := r.b[r.pos>>3]&(0x80>>(r.pos&7)) != 0
	r.pos++
	return bit, nil
}

func (r *bstreamReader) readBits(n int) (uint64, error) {
	if r.pos+uint64(n) > r.end {
		return 0, io.EOF
	}
	var u uint64
	// Fast path for whole bytes when byte-aligned.
	for n >= 8 && r.pos&7 == 0 {
		u = u<<8 | uint64(r.b[r.pos>>3])
		r.pos += 8
		n -= 8
	}
	for ; n > 0; n-- {
		u <<= 1
		if r.b[r.pos>>3]&(0x80>>(r.pos&7)) != 0 {
			u |= 1
		}
		r.pos++
	}
	return u, nil
}
