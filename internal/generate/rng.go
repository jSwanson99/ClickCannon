package generate

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand/v2"
)

const hexChars = "0123456789abcdef"
const alphaNumChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// Rng is a seeded random number generator with helper methods for generating
// common string types. Each worker gets its own Rng for deterministic, lock-free generation.
type Rng struct {
	*rand.Rand
	buf [128]byte // reusable buffer for string generation
}

// NewRng creates a new seeded RNG from an app seed string and worker ID.
func NewRng(seed string, workerID int) *Rng {
	h := fnv.New64a()
	h.Write([]byte(seed))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(workerID))
	h.Write(b)

	return &Rng{
		Rand: rand.New(rand.NewPCG(h.Sum64(), uint64(workerID))),
	}
}

// HexString generates a random lowercase hex string of length n.
func (r *Rng) HexString(n int) string {
	var buf []byte
	if n <= len(r.buf) {
		buf = r.buf[:n]
	} else {
		buf = make([]byte, n)
	}
	for i := range buf {
		buf[i] = hexChars[r.IntN(16)]
	}
	return string(buf)
}

// AlphaNum generates a random alphanumeric string of length n.
func (r *Rng) AlphaNum(n int) string {
	var buf []byte
	if n <= len(r.buf) {
		buf = r.buf[:n]
	} else {
		buf = make([]byte, n)
	}
	for i := range buf {
		buf[i] = alphaNumChars[r.IntN(len(alphaNumChars))]
	}
	return string(buf)
}

// UUID generates a random UUID v4 string.
func (r *Rng) UUID() string {
	var u [16]byte
	r.Read(u[:])
	u[6] = (u[6] & 0x0f) | 0x40 // version 4
	u[8] = (u[8] & 0x3f) | 0x80 // variant 10
	buf := r.buf[:36]
	hexEncode(buf[0:8], u[0:4])
	buf[8] = '-'
	hexEncode(buf[9:13], u[4:6])
	buf[13] = '-'
	hexEncode(buf[14:18], u[6:8])
	buf[18] = '-'
	hexEncode(buf[19:23], u[8:10])
	buf[23] = '-'
	hexEncode(buf[24:36], u[10:16])
	return string(buf)
}

// TraceID generates a valid OTel Trace ID (32 lowercase hex chars, non-zero).
func (r *Rng) TraceID() string {
	return r.HexString(32)
}

// SpanID generates a valid OTel Span ID (16 lowercase hex chars, non-zero).
func (r *Rng) SpanID() string {
	return r.HexString(16)
}

// Read fills p with random bytes from the RNG.
func (r *Rng) Read(p []byte) {
	for i := 0; i < len(p); i += 8 {
		val := r.Uint64()
		for j := 0; j < 8 && i+j < len(p); j++ {
			p[i+j] = byte(val >> (j * 8))
		}
	}
}

func hexEncode(dst []byte, src []byte) {
	for i, b := range src {
		dst[i*2] = hexChars[b>>4]
		dst[i*2+1] = hexChars[b&0x0f]
	}
}
