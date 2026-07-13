package handlers

import (
	"encoding/binary"
	"gtsdb/models"
	"math"
	"net"
)

// Binary protocol for fast read responses.
// Wire format: [uint32 frame_length (big-endian)] [frame_data...]
// Frame data:
//   [uint32] number_of_keys (big-endian)
//   For each key:
//     [uint16] key_length (big-endian)
//     [N bytes] key (UTF-8)
//     [uint32] point_count (big-endian)
//     For each point:
//       [int64] timestamp (big-endian)
//       [float64] value (big-endian, IEEE 754)

// writeBinaryMultiData writes MultiData with length-prefix framing.
func writeBinaryMultiData(conn net.Conn, multiData map[string][]models.DataPoint) error {
	totalSize := 4 // key count
	for k, pts := range multiData {
		totalSize += 2 + len(k) + 4 // key header
		totalSize += len(pts) * 16  // 8 bytes ts + 8 bytes value
	}

	// 4 bytes length prefix + frame data
	buf := make([]byte, 4, 4+totalSize)
	tmp := make([]byte, 8)

	// Key count
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(multiData)))

	for key, points := range multiData {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(key)))
		buf = append(buf, key...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(points)))
		for _, dp := range points {
			binary.BigEndian.PutUint64(tmp, uint64(dp.Timestamp))
			buf = append(buf, tmp...)
			binary.BigEndian.PutUint64(tmp, math.Float64bits(dp.Value))
			buf = append(buf, tmp...)
		}
	}

	// Write length prefix
	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-4))
	_, err := conn.Write(buf)
	return err
}

// writeBinaryDataPoints writes a slice of DataPoint with length-prefix framing.
func writeBinaryDataPoints(conn net.Conn, key string, points []models.DataPoint) error {
	totalSize := 4 + 2 + len(key) + 4 + len(points)*16
	buf := make([]byte, 4, 4+totalSize)
	tmp := make([]byte, 8)

	// 1 key
	buf = binary.BigEndian.AppendUint32(buf, 1)
	// Key
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(key)))
	buf = append(buf, key...)
	// Points
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(points)))
	for _, dp := range points {
		binary.BigEndian.PutUint64(tmp, uint64(dp.Timestamp))
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint64(tmp, math.Float64bits(dp.Value))
		buf = append(buf, tmp...)
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-4))
	_, err := conn.Write(buf)
	return err
}
