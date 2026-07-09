package buffer

import (
	"encoding/binary"
	"fmt"
	"math"

	"gtsdb/models"
)

// Gorilla time-series compressor for (timestamp, value) pairs.
// Based on Facebook Gorilla paper: delta-of-delta timestamps + XOR values.
// Block format: [first_ts:int64][first_val:float64][n_records:uint32][compressed_body...]

type bitWriter struct {
	buf []byte
	pos int // bit position (0-7 within byte)
}

func newBitWriter(capacity int) *bitWriter {
	return &bitWriter{buf: make([]byte, 0, capacity)}
}

func (w *bitWriter) writeBit(b byte) {
	byteIdx := w.pos >> 3
	bitIdx := w.pos & 7
	if byteIdx >= len(w.buf) {
		w.buf = append(w.buf, 0)
	}
	if b != 0 {
		w.buf[byteIdx] |= 1 << (7 - bitIdx)
	}
	w.pos++
}

func (w *bitWriter) writeBits(val uint64, nBits int) {
	for i := nBits - 1; i >= 0; i-- {
		w.writeBit(byte((val >> i) & 1))
	}
}

func (w *bitWriter) bytes() []byte {
	return w.buf
}

type bitReader struct {
	buf []byte
	pos int // bit position
}

func newBitReader(buf []byte) *bitReader {
	return &bitReader{buf: buf, pos: 0}
}

func (r *bitReader) readBit() byte {
	byteIdx := r.pos >> 3
	bitIdx := r.pos & 7
	if byteIdx >= len(r.buf) {
		return 0
	}
	r.pos++
	return (r.buf[byteIdx] >> (7 - bitIdx)) & 1
}

func (r *bitReader) readBits(nBits int) uint64 {
	var val uint64
	for i := 0; i < nBits; i++ {
		val = (val << 1) | uint64(r.readBit())
	}
	return val
}

// EncodeBlock compresses a block of data points using Gorilla encoding.
// Returns the compressed bytes (header + body).
func EncodeBlock(points []models.DataPoint) ([]byte, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("empty block")
	}

	// Write 20-byte header: first timestamp + first value + record count
	header := make([]byte, 20)
	binary.LittleEndian.PutUint64(header[0:8], uint64(points[0].Timestamp))
	binary.LittleEndian.PutUint64(header[8:16], math.Float64bits(points[0].Value))
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(points)))

	w := newBitWriter(256)

	var prevTs = points[0].Timestamp
	var prevDelta int64 = 0
	var prevVal = math.Float64bits(points[0].Value)

	for i := 1; i < len(points); i++ {
		// --- Timestamp: delta-of-delta ---
		delta := points[i].Timestamp - prevTs
		dod := delta - prevDelta
		prevTs = points[i].Timestamp
		prevDelta = delta

		switch {
		case dod == 0:
			w.writeBit(0) // 1 bit
		case dod >= -63 && dod <= 64:
			w.writeBits(0x2, 2) // '10'
			// 7 bits for value (zigzag-like: add 63 offset)
			w.writeBits(uint64(dod+63), 7)
		case dod >= -255 && dod <= 256:
			w.writeBits(0x6, 3) // '110'
			w.writeBits(uint64(dod+255), 9)
		case dod >= -2047 && dod <= 2048:
			w.writeBits(0xE, 4) // '1110'
			w.writeBits(uint64(dod+2047), 12)
		default:
			w.writeBits(0xF, 4) // '1111'
			w.writeBits(uint64(dod), 32)
		}

		// --- Value: XOR ---
		currVal := math.Float64bits(points[i].Value)
		xor := currVal ^ prevVal
		prevVal = currVal

		if xor == 0 {
			w.writeBit(0) // 1 bit: value unchanged
		} else {
			w.writeBit(1) // '1'

			leadingZeros := uint64(clz(xor))
			trailingZeros := uint64(ctz(xor))
			meaningfulBits := 64 - leadingZeros - trailingZeros

			if leadingZeros >= 32 {
				leadingZeros = 31
			}

			// Write '10' header (same leading/trailing zeros)
			// or '11' header (different)
			if i > 1 {
				// simplified: always use '11' for full precision with 5-bit lead + 6-bit meaningful
				// This isn't the optimal Gorilla approach but keeps code simple
				w.writeBit(1) // '11'
				w.writeBits(leadingZeros, 5)
				w.writeBits(meaningfulBits-1, 6) // store meaningfulBits-1 (0-63)
				w.writeBits(xor>>trailingZeros, int(meaningfulBits))
			} else {
				// first XOR: no previous leading/trailing to compare with
				w.writeBit(1) // '11' pattern
				w.writeBits(leadingZeros, 5)
				w.writeBits(meaningfulBits-1, 6)
				w.writeBits(xor>>trailingZeros, int(meaningfulBits))
			}
		}
	}

	// Combine header + compressed body
	result := make([]byte, 20+len(w.bytes()))
	copy(result[0:20], header)
	copy(result[20:], w.bytes())
	return result, nil
}

// DecodeBlock decompresses a Gorilla-encoded block back into data points.
func DecodeBlock(data []byte) ([]models.DataPoint, error) {
	if len(data) < 20 {
		return nil, fmt.Errorf("block too short: %d bytes", len(data))
	}

	// Read header
	firstTs := int64(binary.LittleEndian.Uint64(data[0:8]))
	firstVal := math.Float64frombits(binary.LittleEndian.Uint64(data[8:16]))
	numRecords := int(binary.LittleEndian.Uint32(data[16:20]))

	result := make([]models.DataPoint, 0, numRecords)
	result = append(result, models.DataPoint{Timestamp: firstTs, Value: firstVal})

	var prevTs = firstTs
	var prevDelta int64 = 0
	var prevVal = math.Float64bits(firstVal)

	r := newBitReader(data[20:])

	// Decode exactly numRecords-1 more points (first already added above)
	for i := 1; i < numRecords; i++ {
		// --- Timestamp ---
		firstBit := r.readBit()
		var dod int64
		if firstBit == 0 {
			dod = 0
		} else {
			secondBit := r.readBit()
			if secondBit == 0 {
				// '10' pattern: 7-bit value in [-63, 64]
				dod = int64(r.readBits(7)) - 63
			} else {
				thirdBit := r.readBit()
				if thirdBit == 0 {
					// '110' pattern: 9-bit value in [-255, 256]
					dod = int64(r.readBits(9)) - 255
				} else {
					fourthBit := r.readBit()
					if fourthBit == 0 {
						// '1110' pattern: 12-bit value in [-2047, 2048]
						dod = int64(r.readBits(12)) - 2047
					} else {
						// '1111' pattern: full 32-bit
						dod = int64(int32(r.readBits(32)))
					}
				}
			}
		}

		delta := prevDelta + dod
		ts := prevTs + delta
		prevTs = ts
		prevDelta = delta

		// --- Value ---
		xorHeader := r.readBit()
		var val uint64
		if xorHeader == 0 {
			// Value unchanged
			val = prevVal
		} else {
			r.readBit() // '1' in '11' pattern
			leadingZeros := r.readBits(5)
			meaningfulBits := r.readBits(6) + 1

			xor := r.readBits(int(meaningfulBits))
			xor <<= (64 - leadingZeros - meaningfulBits)
			val = prevVal ^ xor
		}
		prevVal = val

		result = append(result, models.DataPoint{
			Timestamp: ts,
			Value:     math.Float64frombits(val),
		})
	}

	return result, nil
}

// clz returns count of leading zeros in a uint64.
func clz(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	if x&0xFFFFFFFF00000000 == 0 {
		n += 32
		x <<= 32
	}
	if x&0xFFFF000000000000 == 0 {
		n += 16
		x <<= 16
	}
	if x&0xFF00000000000000 == 0 {
		n += 8
		x <<= 8
	}
	if x&0xF000000000000000 == 0 {
		n += 4
		x <<= 4
	}
	if x&0xC000000000000000 == 0 {
		n += 2
		x <<= 2
	}
	if x&0x8000000000000000 == 0 {
		n++
	}
	return n
}

// ctz returns count of trailing zeros in a uint64.
func ctz(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	if x&0xFFFFFFFF == 0 {
		n += 32
		x >>= 32
	}
	if x&0xFFFF == 0 {
		n += 16
		x >>= 16
	}
	if x&0xFF == 0 {
		n += 8
		x >>= 8
	}
	if x&0xF == 0 {
		n += 4
		x >>= 4
	}
	if x&0x3 == 0 {
		n += 2
		x >>= 2
	}
	if x&0x1 == 0 {
		n++
	}
	return n
}
