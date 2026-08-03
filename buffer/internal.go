package buffer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/utils"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// writeRecord writes a timestamp (int64) and value (float64) as a single 16-byte record.
// Avoids reflection overhead of binary.Write by using direct encoding.
func writeRecord(file *os.File, timestamp int64, value float64) error {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(timestamp))
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(value))
	_, err := file.Write(buf[:])
	if err != nil {
		return fmt.Errorf("error writing record to file: %w", err)
	}
	return nil
}

// writeIndexEntry writes a timestamp (int64) and file offset (int64) as a 16-byte index entry.
func writeIndexEntry(file *os.File, timestamp int64, offset int64) error {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(timestamp))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(offset))
	_, err := file.Write(buf[:])
	if err != nil {
		return fmt.Errorf("error writing index entry to file: %w", err)
	}
	return nil
}

// readRecord reads a 16-byte record into timestamp and value pointers.
// Avoids reflection overhead of binary.Read by using direct decoding.
func readRecord(reader io.Reader, timestamp *int64, value *float64) error {
	var buf [16]byte
	_, err := io.ReadFull(reader, buf[:])
	if err != nil {
		return err
	}
	*timestamp = int64(binary.LittleEndian.Uint64(buf[0:8]))
	*value = math.Float64frombits(binary.LittleEndian.Uint64(buf[8:16]))
	return nil
}

// readIndexEntry reads a 16-byte index entry.
func readIndexEntry(reader io.Reader, timestamp *int64, offset *int64) error {
	var buf [16]byte
	_, err := io.ReadFull(reader, buf[:])
	if err != nil {
		return err
	}
	*timestamp = int64(binary.LittleEndian.Uint64(buf[0:8]))
	*offset = int64(binary.LittleEndian.Uint64(buf[8:16]))
	return nil
}

// refFile wraps an *os.File with reference counting so the LRU can close a
// file only when no operation is currently using it.
type refFile struct {
	file         *os.File
	refs         atomic.Int32
	pendingClose atomic.Bool
	closed       atomic.Bool
}

// acquire increments the reference count. Only called under the LRU lock,
// so it never races with closeIfIdle.
func (r *refFile) acquire() {
	r.refs.Add(1)
}

// release decrements the reference count and closes the file if an eviction
// or shutdown requested a close while the file was still in use.
//
// Every acquire must be paired with exactly one release (typically via
// defer immediately after the acquire). A release without a matching
// acquire drives the counter negative; that is a programming error and is
// logged loudly instead of silently corrupting the refcount.
func (r *refFile) release() {
	if r.refs.Add(-1) < 0 {
		name := "?"
		if r.file != nil {
			name = r.file.Name()
		}
		utils.Error("refFile.release without matching acquire: %s", name)
		return
	}
	if r.refs.Load() == 0 && r.pendingClose.Load() {
		r.closeNow()
	}
}

// closeIfIdle closes the file immediately if no references are held,
// otherwise defers the close until the last reference is released.
// Only called under the LRU lock, so the refcount cannot change concurrently.
func (r *refFile) closeIfIdle() {
	if r.refs.Load() == 0 {
		r.closeNow()
	} else {
		r.pendingClose.Store(true)
	}
}

func (r *refFile) closeNow() {
	if r.closed.CompareAndSwap(false, true) {
		if err := r.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			utils.Error("Error closing file handle: %v", err)
		}
	}
}

func storeDataPoints(dataPointId string, dataPoints []models.DataPoint) {
	lock, _ := fileWriteLocks.LoadOrStore(dataPointId, &sync.Mutex{})
	lock.Lock()
	defer lock.Unlock()

	dataRef, ok := acquireFileHandle(dataPointId+".aof", dataFileHandles)
	if !ok {
		utils.Error("Cannot open data file for %s, skipping write", dataPointId)
		return
	}
	defer dataRef.release()
	dataFile := dataRef.file

	indexRef, _ := acquireFileHandle(dataPointId+".idx", indexFileHandles)
	var indexFile *os.File
	if indexRef != nil {
		defer indexRef.release()
		indexFile = indexRef.file
	}

	// Fast path: batch-write all points using a pre-allocated buffer.
	// This reduces N individual 16-byte Write() syscalls to 1 large write.
	if len(dataPoints) > 1 {
		buf := make([]byte, len(dataPoints)*16)
		for i, dp := range dataPoints {
			off := i * 16
			binary.LittleEndian.PutUint64(buf[off:off+8], uint64(dp.Timestamp))
			binary.LittleEndian.PutUint64(buf[off+8:off+16], math.Float64bits(dp.Value))
		}
		if _, err := dataFile.Write(buf); err != nil {
			utils.Error("Failed to batch-write data points for %s: %v", dataPointId, err)
			return
		}

		// Update counts and index entries (one index entry per indexInterval)
		countValue, _ := idToCountMap.Load(dataPointId)
		count := countValue
		newCount := count.Add(int64(len(dataPoints)))
		totalDataPoints.Add(int64(len(dataPoints)))

		// Build index entries if needed
		if indexFile != nil {
			offset, _ := dataFile.Seek(0, io.SeekEnd)
			recordStart := offset - int64(len(buf))
			for i, dp := range dataPoints {
				if (newCount-int64(len(dataPoints))+int64(i)+1)%indexInterval == 0 {
					entryOff := recordStart + int64(i*16)
					if err := writeIndexEntry(indexFile, dp.Timestamp, entryOff); err != nil {
						utils.Error("Failed to update index for %s: %v", dataPointId, err)
					}
				}
			}
		}

		if utils.SyncMode == "sync" {
			if err := dataFile.Sync(); err != nil {
				utils.Error("Failed to sync data file for %s: %v", dataPointId, err)
			}
		} else {
			dirtyKeys.Add(dataPointId)
		}
		return
	}

	// Slow path: single point (original code path for minimal overhead)
	for _, dataPoint := range dataPoints {
		if err := writeRecord(dataFile, dataPoint.Timestamp, dataPoint.Value); err != nil {
			utils.Error("Failed to write data point for %s: %v", dataPointId, err)
			return
		}

		countValue, _ := idToCountMap.Load(dataPointId)
		count := countValue
		newCount := count.Add(1)
		totalDataPoints.Add(1)

		if newCount%indexInterval == 0 {
			offset, _ := dataFile.Seek(0, io.SeekEnd)
			offset -= int64(binary.Size(dataPoint.Timestamp) + binary.Size(dataPoint.Value))
			if err := updateIndexFile(indexFile, dataPoint.Timestamp, offset); err != nil {
				utils.Error("Failed to update index for %s: %v", dataPointId, err)
			}
		}
	}
	// Only sync if in legacy sync mode; async flusher handles it otherwise
	if utils.SyncMode == "sync" {
		if err := dataFile.Sync(); err != nil {
			utils.Error("Failed to sync data file for %s: %v", dataPointId, err)
		}
	} else {
		dirtyKeys.Add(dataPointId)
	}
}

// acquireFileHandle returns a reference-counted handle for fileName, opening it
// if necessary. Callers must release() the handle when done.
// The reference count is incremented while holding the LRU lock so eviction
// cannot close a file that is about to be used.
//
// CONTRACT: every successful acquire (ok == true) must be paired with exactly
// one release(), typically `defer ref.release()` placed immediately after the
// acquire so early returns cannot leak. A forgotten release keeps the file
// open forever (fd leak; on Windows the file also cannot be renamed/deleted).
func acquireFileHandle(fileName string, handleMap *concurrent.LRU[string, *refFile]) (*refFile, bool) {
	return handleMap.GetOrCreateRef(fileName, func() (*refFile, bool) {
		fullPath := utils.DataDir + "/" + fileName
		dir := filepath.Dir(fullPath)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				utils.Error("Error creating directory %s: %v", dir, err)
				return nil, false
			}
		}

		file, err := os.OpenFile(fullPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			utils.Error("Error opening file %s: %v", fullPath, err)
			return nil, false
		}

		ref := &refFile{file: file}

		if strings.HasSuffix(fileName, ".aof") {
			key := fileName[:len(fileName)-4]
			if _, ok := idToCountMap.Load(key); !ok {
				fileInfo, err := file.Stat()
				if err != nil {
					utils.Error("Error getting file info for %s: %v", fullPath, err)
					return ref, true
				}
				fileLength := fileInfo.Size()
				count := &atomic.Int64{}
				count.Store(fileLength / 16)
				idToCountMap.Store(key, count)
				totalDataPoints.Add(fileLength / 16)
			}
		}
		return ref, true
	}, (*refFile).acquire)
}

// refFromLRU acquires a reference to an existing LRU entry without creating it.
func refFromLRU(l *concurrent.LRU[string, *refFile], key string) (*refFile, bool) {
	return l.GetRef(key, (*refFile).acquire)
}

// primeFileHandle opens a file into the LRU cache without keeping a
// reference. Used to warm the cache (e.g. after rename/reload/compact); the
// acquire/release pairing is contained here, so callers cannot leak.
func primeFileHandle(fileName string, handleMap *concurrent.LRU[string, *refFile]) {
	if ref, ok := acquireFileHandle(fileName, handleMap); ok {
		ref.release()
	}
}

func readLastFiledDataPoints(id string, count int) ([]models.DataPoint, error) {
	ref, ok := acquireFileHandle(id+".aof", dataFileHandles)
	if !ok {
		return nil, nil
	}
	defer ref.release()
	file := ref.file

	// Use atomic counter for O(1) size instead of file.Stat() syscall.
	actualRecordCount := int64(0)
	if cv, ok := idToCountMap.Load(id); ok {
		actualRecordCount = cv.Load()
	}
	if actualRecordCount == 0 {
		return nil, nil
	}
	if int64(count) > actualRecordCount {
		count = int(actualRecordCount)
	}

	// Seek to last N records and batch-read them in one call
	bufSize := count * 16
	buf := make([]byte, bufSize)

	// Use ReadAt to avoid O_APPEND seek issues on Windows
	seekPosition := (actualRecordCount - int64(count)) * 16
	n, err := file.ReadAt(buf, seekPosition)
	if err != nil && err != io.EOF {
		utils.Error("Error reading file: %v", err)
		return nil, err
	}

	// Decode records from buffer
	dataPoints := make([]models.DataPoint, 0, n/16)
	for i := 0; i+16 <= n; i += 16 {
		ts := int64(binary.LittleEndian.Uint64(buf[i : i+8]))
		val := math.Float64frombits(binary.LittleEndian.Uint64(buf[i+8 : i+16]))
		dataPoints = append(dataPoints, models.DataPoint{
			Key:       id,
			Timestamp: ts,
			Value:     val,
		})
	}

	return dataPoints, nil
}

func updateIndexFile(indexFile *os.File, timestamp int64, offset int64) error {
	return writeIndexEntry(indexFile, timestamp, offset)
}

// findStartOffset scans the index file for the last entry whose timestamp is
// <= target and returns the data-file offset where scanning should begin.
// A missing or empty index means the scan must start at offset 0.
func findStartOffset(id string, target int64) int64 {
	indexRef, ok := acquireFileHandle(id+".idx", indexFileHandles)
	if !ok {
		return 0
	}
	defer indexRef.release()

	fileInfo, err := indexRef.file.Stat()
	if err != nil {
		return 0
	}
	size := fileInfo.Size()
	if size == 0 {
		return 0
	}

	buf := make([]byte, size)
	if _, err := indexRef.file.ReadAt(buf, 0); err != nil && err != io.EOF {
		utils.Error("Error reading index file for %s: %v", id, err)
		return 0
	}

	offset := int64(0)
	for i := 0; i+16 <= len(buf); i += 16 {
		ts := int64(binary.LittleEndian.Uint64(buf[i : i+8]))
		if ts > target {
			break
		}
		offset = int64(binary.LittleEndian.Uint64(buf[i+8 : i+16]))
	}
	return offset
}

// readFiledDataPoints reads a timestamp range from the on-disk WAL using
// ReadAt, which is safe for concurrent readers sharing a file handle.
func readFiledDataPoints(id string, startTime int64, endTime int64) []models.DataPoint {
	dataRef, ok := acquireFileHandle(id+".aof", dataFileHandles)
	if !ok {
		return nil
	}
	defer dataRef.release()

	// O(1) size from the in-memory counter; 0 means the file is empty.
	endOffset := int64(0)
	if cv, ok := idToCountMap.Load(id); ok {
		endOffset = cv.Load() * 16
	}
	if endOffset == 0 {
		return nil
	}

	startOffset := findStartOffset(id, startTime)
	if startOffset >= endOffset {
		return nil
	}

	dataPoints := make([]models.DataPoint, 0, (endOffset-startOffset)/16)
	buf := make([]byte, 64*1024)
	pos := startOffset
	for pos < endOffset {
		toRead := int64(len(buf))
		if endOffset-pos < toRead {
			toRead = endOffset - pos
		}
		n, err := dataRef.file.ReadAt(buf[:toRead], pos)
		if err != nil && err != io.EOF {
			utils.Error("Error reading file %s: %v", id, err)
			return nil
		}

		for i := 0; i+16 <= n; i += 16 {
			ts := int64(binary.LittleEndian.Uint64(buf[i : i+8]))
			if ts > endTime {
				return dataPoints
			}
			if ts >= startTime {
				val := math.Float64frombits(binary.LittleEndian.Uint64(buf[i+8 : i+16]))
				dataPoints = append(dataPoints, models.DataPoint{
					Key:       id,
					Timestamp: ts,
					Value:     val,
				})
			}
		}
		if int64(n) < toRead {
			break
		}
		pos += int64(n)
	}

	return dataPoints
}

func readBufferedDataPoints(id string, startTime, endTime int64) []models.DataPoint {
	if cacheSize == 0 {
		return []models.DataPoint{}
	}

	rb, ok := idToRingBufferMap.Load(id)
	if !ok {
		return []models.DataPoint{}
	}

	result := make([]models.DataPoint, 0, rb.Size())
	for i := 0; i < rb.Size(); i++ {
		dataPoint, _ := rb.Get(i)
		dataPoint.Key = id
		if dataPoint.Timestamp >= startTime && dataPoint.Timestamp <= endTime {
			result = append(result, dataPoint)
		}
	}
	return result
}

func checkIfBufferHasEnoughDataPoints(id string, count int) bool {
	rb, ok := idToRingBufferMap.Load(id)
	if !ok {
		return false
	}
	return rb.Size() >= count
}

func readLastBufferedDataPoints(id string, count int) []models.DataPoint {
	if count == 1 {
		timestampValue, ok := lastTimestamp.Load(id)
		if ok && timestampValue != 0 {
			value, _ := lastValue.Load(id)
			return []models.DataPoint{{Timestamp: timestampValue, Value: value, Key: id}}
		}
	}

	rb, ok := idToRingBufferMap.Load(id)
	if !ok {
		return []models.DataPoint{}
	}

	// Single lock acquisition for the entire batch read
	return rb.GetLast(count)
}

func downsampleDataPoints(dataPoints []models.DataPoint, downsample int, aggregation string) []models.DataPoint {
	if len(dataPoints) == 0 {
		return dataPoints
	}

	needsValueCollection := aggregation == "median" || aggregation == "p50" || aggregation == "p95" || aggregation == "p99"

	var downsampled []models.DataPoint
	intervalStart := dataPoints[0].Timestamp
	intervalSum := dataPoints[0].Value
	intervalCount := 1
	intervalMin := dataPoints[0].Value
	intervalMax := dataPoints[0].Value
	intervalFirst := dataPoints[0].Value
	intervalLast := dataPoints[0].Value
	var intervalValues []float64
	if needsValueCollection {
		intervalValues = []float64{dataPoints[0].Value}
	}

	for i := 1; i < len(dataPoints); i++ {
		dp := dataPoints[i]
		if dp.Timestamp-intervalStart >= int64(downsample) {
			// flush current interval
			value := computeAggregate(aggregation, intervalSum, float64(intervalCount), intervalMin, intervalMax, intervalFirst, intervalLast, intervalValues)
			downsampled = append(downsampled, models.DataPoint{
				Key: dp.Key, Timestamp: intervalStart, Value: value,
			})
			// reset interval
			intervalStart = dp.Timestamp
			intervalSum = dp.Value
			intervalCount = 1
			intervalMin = dp.Value
			intervalMax = dp.Value
			intervalFirst = dp.Value
			intervalLast = dp.Value
			if needsValueCollection {
				intervalValues = []float64{dp.Value}
			}
		} else {
			intervalSum += dp.Value
			intervalCount++
			if dp.Value < intervalMin {
				intervalMin = dp.Value
			}
			if dp.Value > intervalMax {
				intervalMax = dp.Value
			}
			intervalLast = dp.Value
			if needsValueCollection {
				intervalValues = append(intervalValues, dp.Value)
			}
		}
	}

	// flush final interval
	if intervalCount > 0 {
		lastDp := dataPoints[len(dataPoints)-1]
		value := computeAggregate(aggregation, intervalSum, float64(intervalCount), intervalMin, intervalMax, intervalFirst, intervalLast, intervalValues)
		downsampled = append(downsampled, models.DataPoint{
			Key: lastDp.Key, Timestamp: intervalStart, Value: value,
		})
	}

	return downsampled
}

// computeAggregate calculates the aggregate value for an interval.
// Separated to avoid closure allocation in the hot path.
func computeAggregate(aggregation string, sum, count float64, min, max, first, last float64, values []float64) float64 {
	switch aggregation {
	case "avg":
		return sum / count
	case "sum":
		return sum
	case "min":
		return min
	case "max":
		return max
	case "first":
		return first
	case "last":
		return last
	case "count":
		return count
	case "median", "p50":
		if len(values) > 0 {
			sort.Float64s(values)
			return values[len(values)/2]
		}
		return 0
	case "p95":
		if len(values) > 0 {
			sort.Float64s(values)
			idx := int(float64(len(values)) * 0.95)
			if idx >= len(values) {
				idx = len(values) - 1
			}
			return values[idx]
		}
		return 0
	case "p99":
		if len(values) > 0 {
			sort.Float64s(values)
			idx := int(float64(len(values)) * 0.99)
			if idx >= len(values) {
				idx = len(values) - 1
			}
			return values[idx]
		}
		return 0
	default:
		return sum / count
	}
}

// InitFileHandles initializes the file handle LRUs with the configured capacity
func InitFileHandles() {
	capacity := utils.FileHandleLRUCapacity

	// Close handles from a previous initialization (e.g. tests re-initializing)
	if dataFileHandles != nil {
		dataFileHandles.Clear()
	}
	if indexFileHandles != nil {
		indexFileHandles.Clear()
	}

	dataFileHandles = concurrent.NewLRUWithEvict(capacity, func(_ string, ref *refFile) {
		if ref != nil {
			ref.closeIfIdle()
		}
	})

	indexFileHandles = concurrent.NewLRUWithEvict(capacity, func(_ string, ref *refFile) {
		if ref != nil {
			ref.closeIfIdle()
		}
	})

	utils.Logln("Handle LRU 容量：", capacity)
}

// CloseAllHandles closes all open file handles. Files still in use are closed
// as soon as the last operation releases them. Used at shutdown.
func CloseAllHandles() {
	if dataFileHandles != nil {
		dataFileHandles.Clear()
	}
	if indexFileHandles != nil {
		indexFileHandles.Clear()
	}
}

// GetDataFileSize returns the size of a data file if its handle is currently
// open. The reference is acquired and released internally so callers never
// hold a raw handle.
func GetDataFileSize(fileName string) (int64, bool) {
	ref, ok := refFromLRU(dataFileHandles, fileName)
	if !ok {
		return 0, false
	}
	defer ref.release()
	stat, err := ref.file.Stat()
	if err != nil {
		return 0, false
	}
	return stat.Size(), true
}

// SetCacheSize configures the in-memory ring buffer size per key for fast reads.
// Set to 0 to disable (reads go to disk).
func SetCacheSize(size int) {
	cacheSize = size
}
