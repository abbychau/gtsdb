package buffer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"gtsdb/models"
	"gtsdb/utils"
	"io"
	"os"
)

const gorillaBlockSize = indexInterval // 5000 points per block

// writeCompressedWAL writes a Gorilla-compressed version of all data points for a key.
// Output: key.aof.gor (compressed blocks) + key.idx (same format, byte offsets into .gor)
func writeCompressedWAL(key string, dataPoints []models.DataPoint) error {
	gorFile := utils.DataDir + "/" + key + ".aof.gor.tmp"
	idxFile := utils.DataDir + "/" + key + ".idx.tmp"

	os.Remove(gorFile)
	os.Remove(idxFile)

	gorHandle, err := os.OpenFile(gorFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compressed file: %w", err)
	}
	defer gorHandle.Close()

	idxHandle, err := os.OpenFile(idxFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compressed index: %w", err)
	}
	defer idxHandle.Close()

	byteOffset := int64(0)

	// Split into blocks of gorillaBlockSize and compress each
	for start := 0; start < len(dataPoints); start += gorillaBlockSize {
		end := start + gorillaBlockSize
		if end > len(dataPoints) {
			end = len(dataPoints)
		}
		block := dataPoints[start:end]

		compressed, err := EncodeBlock(block)
		if err != nil {
			return fmt.Errorf("failed to encode block: %w", err)
		}

		// Write block length prefix (4 bytes) + compressed data
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(compressed)))
		if _, err := gorHandle.Write(lenBuf[:]); err != nil {
			return fmt.Errorf("failed to write block header: %w", err)
		}
		if _, err := gorHandle.Write(compressed); err != nil {
			return fmt.Errorf("failed to write compressed block: %w", err)
		}

		// Write index entry: timestamp of first point → byte offset of block start
		var idxBuf [16]byte
		binary.LittleEndian.PutUint64(idxBuf[0:8], uint64(block[0].Timestamp))
		binary.LittleEndian.PutUint64(idxBuf[8:16], uint64(byteOffset))
		if _, err := idxHandle.Write(idxBuf[:]); err != nil {
			return fmt.Errorf("failed to write index entry: %w", err)
		}

		byteOffset += int64(4 + len(compressed))
	}

	gorHandle.Close()
	idxHandle.Close()

	// Atomic rename
	realGor := utils.DataDir + "/" + key + ".aof.gor"
	if err := os.Rename(gorFile, realGor); err != nil {
		os.Remove(gorFile)
		os.Remove(idxFile)
		return fmt.Errorf("failed to rename compressed file: %w", err)
	}

	utils.Log("Compressed WAL for %s: %d points → %d bytes (%d blocks)",
		key, len(dataPoints), byteOffset, (len(dataPoints)+gorillaBlockSize-1)/gorillaBlockSize)

	return nil
}

// readCompressedDataPoints reads data points from a Gorilla-compressed WAL file.
func readCompressedDataPoints(id string, startTime, endTime int64) ([]models.DataPoint, error) {
	gorFile := utils.DataDir + "/" + id + ".aof.gor"
	if _, err := os.Stat(gorFile); os.IsNotExist(err) {
		return nil, nil // no compressed file
	}

	// Use the index file to find the right block
	idxFile := utils.DataDir + "/" + id + ".idx"
	idxHandle, err := os.Open(idxFile)
	if err != nil {
		return nil, err
	}
	defer idxHandle.Close()

	// Binary search the index to find the starting block
	var blockOffset int64
	idxReader := bufio.NewReaderSize(idxHandle, 64*1024)
	lastOffset := int64(0)
	for {
		var ts int64
		var off int64
		err := readIndexEntry(idxReader, &ts, &off)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				blockOffset = lastOffset
				break
			}
			return nil, err
		}
		if ts > startTime {
			blockOffset = lastOffset
			break
		}
		lastOffset = off
	}

	// Seek to the block and start reading
	gorHandle, err := os.Open(gorFile)
	if err != nil {
		return nil, err
	}
	defer gorHandle.Close()

	if _, err := gorHandle.Seek(blockOffset, io.SeekStart); err != nil {
		return nil, err
	}

	var allPoints []models.DataPoint

	// Read blocks sequentially, decompress, and filter by time range
	gorReader := bufio.NewReaderSize(gorHandle, 256*1024)
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(gorReader, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		blockLen := int(binary.LittleEndian.Uint32(lenBuf[:]))

		blockData := make([]byte, blockLen)
		if _, err := io.ReadFull(gorReader, blockData); err != nil {
			return nil, err
		}

		points, err := DecodeBlock(blockData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode block: %w", err)
		}

		// Filter by time range
		for _, p := range points {
			if p.Timestamp >= startTime && p.Timestamp <= endTime {
				allPoints = append(allPoints, p)
			}
			if p.Timestamp > endTime {
				return allPoints, nil
			}
		}
	}

	return allPoints, nil
}
