package buffer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"gtsdb/models"
	"gtsdb/synchronous"
	"gtsdb/utils"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func InitIDSet() {
	// Rebuild the set from disk: clear stale entries from previous
	// initializations (e.g. when the data dir changes between runs).
	allIds.Clear()

	// Read all the files in the data directory recursively
	err := filepath.WalkDir(utils.DataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".aof") {
			relPath, err := filepath.Rel(utils.DataDir, path)
			if err != nil {
				return err
			}
			id := relPath[:len(relPath)-4]
			if id != "" {
				allIds.Add(id)
			}
		}
		return nil
	})

	if err != nil {
		utils.InitDataDirectory()
		return
	}
}

func InitKey(dataPointId string) {
	if dataPointId == "" {
		return
	}
	if ref, ok := acquireFileHandle(dataPointId+".aof", dataFileHandles); ok {
		ref.release()
	}
	if ref, ok := acquireFileHandle(dataPointId+".idx", indexFileHandles); ok {
		ref.release()
	}
	allIds.Add(dataPointId)
}
func RenameKey(dataPointId, newId string) {
	if newId == "" || dataPointId == "" {
		return
	}
	utils.Log("Renaming key: %v to %v", dataPointId, newId)
	renameLock.Lock()
	defer renameLock.Unlock()

	dfk := dataPointId + ".aof"
	ifk := dataPointId + ".idx"
	newDfk := newId + ".aof"
	newIfk := newId + ".idx"

	// Close and remove old file handles (the eviction callback closes when idle)
	dataFileHandles.Delete(dfk)
	indexFileHandles.Delete(ifk)

	// Remove from allIds before renaming
	allIds.Remove(dataPointId)

	// Rename the files
	err1 := os.Rename(utils.DataDir+"/"+dfk, utils.DataDir+"/"+newDfk)
	err2 := os.Rename(utils.DataDir+"/"+ifk, utils.DataDir+"/"+newIfk)

	if err1 != nil || err2 != nil {
		utils.Errorln("Error renaming files:", err1, err2)
		allIds.Add(dataPointId) // restore old ID on failure
		return
	}

	// Transfer in-memory state from old key to new key
	if count, ok := idToCountMap.Load(dataPointId); ok {
		idToCountMap.Store(newId, count)
		idToCountMap.Delete(dataPointId)
	}
	if val, ok := lastValue.Load(dataPointId); ok {
		lastValue.Store(newId, val)
		lastValue.Delete(dataPointId)
	}
	if ts, ok := lastTimestamp.Load(dataPointId); ok {
		lastTimestamp.Store(newId, ts)
		lastTimestamp.Delete(dataPointId)
	}
	if rb, ok := idToRingBufferMap.Load(dataPointId); ok {
		idToRingBufferMap.Store(newId, rb)
		idToRingBufferMap.Delete(dataPointId)
	}
	// Transfer file write lock
	if lock, ok := fileWriteLocks.Load(dataPointId); ok {
		fileWriteLocks.Store(newId, lock)
		fileWriteLocks.Delete(dataPointId)
	}
	// Transfer data patch lock
	if lock, ok := dataPatchLocks.Load(dataPointId); ok {
		dataPatchLocks.Store(newId, lock)
		dataPatchLocks.Delete(dataPointId)
	}

	// Open new file handles so the LRU is primed and counts stay consistent
	if ref, ok := acquireFileHandle(newDfk, dataFileHandles); ok {
		ref.release()
	}
	if ref, ok := acquireFileHandle(newIfk, indexFileHandles); ok {
		ref.release()
	}

	// Add new ID
	allIds.Add(newId)
}

func DeleteKey(dataPointId string) {
	utils.Log("Deleting key: %v", dataPointId)
	if dataPointId == "" {
		return
	}

	renameLock.Lock()
	defer renameLock.Unlock()

	dfk := dataPointId + ".aof"
	ifk := dataPointId + ".idx"

	// Close file handles if they are open (the eviction callback closes when idle)
	dataFileHandles.Delete(dfk)
	indexFileHandles.Delete(ifk)
	idToRingBufferMap.Delete(dataPointId)
	// Subtract deleted points from global counter before clearing the per-key count
	if cnt, ok := idToCountMap.Load(dataPointId); ok {
		totalDataPoints.Add(-cnt.Load())
	}
	idToCountMap.Delete(dataPointId)
	lastValue.Delete(dataPointId)
	lastTimestamp.Delete(dataPointId)
	allIds.Remove(dataPointId)

	// delete the file
	err := os.Remove(utils.DataDir + "/" + dfk)
	if err != nil && !os.IsNotExist(err) {
		utils.Errorln(err)
	}
	err = os.Remove(utils.DataDir + "/" + ifk)
	if err != nil && !os.IsNotExist(err) {
		utils.Errorln(err)
	}
}

func ReloadKey(dataPointId string) bool {
	if dataPointId == "" {
		return false
	}

	renameLock.Lock()
	defer renameLock.Unlock()

	dfk := dataPointId + ".aof"
	ifk := dataPointId + ".idx"

	if ref, ok := refFromLRU(dataFileHandles, dfk); ok {
		ref.release()
	}
	if ref, ok := refFromLRU(indexFileHandles, ifk); ok {
		ref.release()
	}
	dataFileHandles.Delete(dfk)
	indexFileHandles.Delete(ifk)
	idToRingBufferMap.Delete(dataPointId)
	// Subtract old count before reloading (prepareFileHandles will re-add from file size)
	if cnt, ok := idToCountMap.Load(dataPointId); ok {
		totalDataPoints.Add(-cnt.Load())
	}
	idToCountMap.Delete(dataPointId)
	lastValue.Delete(dataPointId)
	lastTimestamp.Delete(dataPointId)

	if _, err := os.Stat(utils.DataDir + "/" + dfk); err != nil {
		if os.IsNotExist(err) {
			allIds.Remove(dataPointId)
			return false
		}
		utils.Errorln(err)
		return false
	}

	if ref, ok := acquireFileHandle(dfk, dataFileHandles); ok {
		ref.release()
	}
	if _, err := os.Stat(utils.DataDir + "/" + ifk); err == nil {
		if ref, ok := acquireFileHandle(ifk, indexFileHandles); ok {
			ref.release()
		}
	}
	allIds.Add(dataPointId)

	return true
}

func StoreDataPointBuffer(dataPoint models.DataPoint) {
	allIds.Add(dataPoint.Key)

	if cacheSize == 0 {
		storeDataPoints(dataPoint.Key, []models.DataPoint{dataPoint})
		lastValue.Store(dataPoint.Key, dataPoint.Value)
		lastTimestamp.Store(dataPoint.Key, dataPoint.Timestamp)
		return
	}

	rb, ok := idToRingBufferMap.Load(dataPoint.Key)
	if !ok {
		newRb := synchronous.NewRingBuffer[models.DataPoint](cacheSize)
		idToRingBufferMap.Store(dataPoint.Key, newRb)
		rb = newRb
	}
	rb.Push(dataPoint)

	storeDataPoints(dataPoint.Key, []models.DataPoint{dataPoint})

	lastValue.Store(dataPoint.Key, dataPoint.Value)
	lastTimestamp.Store(dataPoint.Key, dataPoint.Timestamp)
}

// StoreDataPointsBuffer stores a batch of data points, grouping by key for efficient writes.
// Instead of acquiring per-key locks once per point, it groups points by key and writes
// all points for each key in a single storeDataPoints call, reducing lock contention.
func StoreDataPointsBuffer(dataPoints []models.DataPoint) {
	if len(dataPoints) == 0 {
		return
	}

	// Fast path: single point delegates to existing function
	if len(dataPoints) == 1 {
		StoreDataPointBuffer(dataPoints[0])
		return
	}

	// Group points by key
	keyGroups := make(map[string][]models.DataPoint, 16)
	for _, dp := range dataPoints {
		allIds.Add(dp.Key)
		keyGroups[dp.Key] = append(keyGroups[dp.Key], dp)
	}

	// Write each key's points in one call, then update caches
	for key, points := range keyGroups {
		storeDataPoints(key, points)

		// Update ring buffer cache if enabled
		if cacheSize > 0 {
			rb, ok := idToRingBufferMap.Load(key)
			if !ok {
				newRb := synchronous.NewRingBuffer[models.DataPoint](cacheSize)
				idToRingBufferMap.Store(key, newRb)
				rb = newRb
			}
			for _, dp := range points {
				rb.Push(dp)
			}
		}

		last := points[len(points)-1]
		lastValue.Store(key, last.Value)
		lastTimestamp.Store(key, last.Timestamp)
	}
}

func PatchDataPoints(dataPoints []models.DataPoint, key string) {
	/*
		1. sort input data points by timestamp
		2. get all data points from key
		3. merge input data points with existing data points
		4. write merged data points to file
		5. rebuild index file
	*/

	lock, _ := dataPatchLocks.LoadOrStore(key, &sync.Mutex{}) //ignore the second return value because we don't care if it was loaded
	lock.Lock()
	defer lock.Unlock()

	// sort input data points by timestamp
	sort.Slice(dataPoints, func(i, j int) bool {
		return dataPoints[i].Timestamp < dataPoints[j].Timestamp
	})

	// Fast path: if patch points are strictly newer than current tail, append directly.
	// This avoids full file read + rewrite for common single-point patch usage.
	if len(dataPoints) > 0 {
		lastTs := int64(0)
		if ts, ok := lastTimestamp.Load(key); ok {
			lastTs = ts
		} else {
			last := ReadLastDataPoints(key, 1)
			if len(last) > 0 {
				lastTs = last[0].Timestamp
			}
		}

		canAppend := true
		for _, p := range dataPoints {
			if p.Timestamp <= lastTs {
				canAppend = false
				break
			}
		}
		if canAppend {
			storeDataPoints(key, dataPoints)
			allIds.Add(key)
			lastValue.Store(key, dataPoints[len(dataPoints)-1].Value)
			lastTimestamp.Store(key, dataPoints[len(dataPoints)-1].Timestamp)
			return
		}
	}

	// Fast path: single-point overwrite when timestamp already exists.
	// This updates one 16-byte record in-place instead of full rewrite.
	if len(dataPoints) == 1 {
		if overwritten := tryOverwriteSingleTimestampValue(key, dataPoints[0]); overwritten {
			return
		}
	}

	// get all data points from key
	existingDataPoints := readFiledDataPoints(key, 0, math.MaxInt64)

	// merge input data points with existing data points
	newDataCursor := 0
	existingDataCursor := 0

	var mergedDataPoints []models.DataPoint

	for newDataCursor < len(dataPoints) && existingDataCursor < len(existingDataPoints) {
		newDataPoint := dataPoints[newDataCursor]
		existingDataPoint := existingDataPoints[existingDataCursor]

		if newDataPoint.Timestamp < existingDataPoint.Timestamp {
			mergedDataPoints = append(mergedDataPoints, newDataPoint)
			newDataCursor++
		} else if newDataPoint.Timestamp > existingDataPoint.Timestamp {
			mergedDataPoints = append(mergedDataPoints, existingDataPoint)
			existingDataCursor++
		} else {
			// Overwrite old data with new data if timestamps are the same
			mergedDataPoints = append(mergedDataPoints, newDataPoint)
			newDataCursor++
			existingDataCursor++
		}
	}

	for newDataCursor < len(dataPoints) {
		mergedDataPoints = append(mergedDataPoints, dataPoints[newDataCursor])
		newDataCursor++
	}

	for existingDataCursor < len(existingDataPoints) {
		mergedDataPoints = append(mergedDataPoints, existingDataPoints[existingDataCursor])
		existingDataCursor++
	}

	rewriteDataPoints(key, mergedDataPoints)
}

func tryOverwriteSingleTimestampValue(key string, point models.DataPoint) bool {
	// Open data file WITHOUT O_APPEND for in-place write (WriteAt is forbidden on O_APPEND files)
	dataFile, err := os.OpenFile(utils.DataDir+"/"+key+".aof", os.O_RDWR, 0644)
	if err != nil {
		return false
	}
	defer dataFile.Close()

	startOffset := findStartOffset(key, point.Timestamp)

	if _, err := dataFile.Seek(startOffset, io.SeekStart); err != nil {
		return false
	}

	reader := bufio.NewReader(dataFile)
	offset := startOffset
	for {
		var ts int64
		var val float64
		err := readRecord(reader, &ts, &val)
		if err != nil {
			return false
		}

		if ts == point.Timestamp {
			// Use WriteAt to write at the exact byte offset, bypassing O_APPEND
			var valBuf [8]byte
			binary.LittleEndian.PutUint64(valBuf[:], math.Float64bits(point.Value))
			if _, err := dataFile.WriteAt(valBuf[:], offset+int64(binary.Size(ts))); err != nil {
				return false
			}
			if err := dataFile.Sync(); err != nil {
				return false
			}

			if lastTs, ok := lastTimestamp.Load(key); ok && lastTs == point.Timestamp {
				lastValue.Store(key, point.Value)
			}
			return true
		}
		if ts > point.Timestamp {
			return false
		}

		offset += int64(binary.Size(ts) + binary.Size(val))
	}
}

func DeleteDataPoints(key, operator string, value float64, hasValue bool, timestampFrom, timestampTo int64) int {
	if key == "" {
		return 0
	}
	if hasValue && operator != ">" && operator != "<" {
		return 0
	}

	lock, _ := dataPatchLocks.LoadOrStore(key, &sync.Mutex{})
	lock.Lock()
	defer lock.Unlock()

	if !allIds.Contains(key) {
		return 0
	}

	existingDataPoints := readFiledDataPoints(key, 0, math.MaxInt64)
	if len(existingDataPoints) == 0 {
		return 0
	}

	filteredDataPoints := make([]models.DataPoint, 0, len(existingDataPoints))
	removedCount := 0

	for _, dataPoint := range existingDataPoints {
		inTimeRange := true
		if timestampFrom > 0 && dataPoint.Timestamp < timestampFrom {
			inTimeRange = false
		}
		if timestampTo > 0 && dataPoint.Timestamp > timestampTo {
			inTimeRange = false
		}
		shouldDelete := inTimeRange
		if hasValue {
			shouldDeleteByValue := (operator == ">" && dataPoint.Value > value) || (operator == "<" && dataPoint.Value < value)
			shouldDelete = shouldDelete && shouldDeleteByValue
		}
		if shouldDelete {
			removedCount++
			continue
		}
		filteredDataPoints = append(filteredDataPoints, dataPoint)
	}

	if removedCount == 0 {
		return 0
	}

	rewriteDataPoints(key, filteredDataPoints)
	return removedCount
}

func rewriteDataPoints(key string, dataPoints []models.DataPoint) {
	DeleteKey(key)

	if len(dataPoints) == 0 {
		return
	}

	// Rewrite the full dataset so the on-disk data stays consistent after patching.
	storeDataPoints(key, dataPoints)
	allIds.Add(key)
	lastValue.Store(key, dataPoints[len(dataPoints)-1].Value)
	lastTimestamp.Store(key, dataPoints[len(dataPoints)-1].Timestamp)
}

func ReadDataPoints(id string, startTime, endTime int64, downsample int, aggregation string) []models.DataPoint {

	dataPoints := readBufferedDataPoints(id, startTime, endTime)
	if len(dataPoints) == 0 {
		// Try compressed WAL first, fall back to raw AOF
		if compressed, err := readCompressedDataPoints(id, startTime, endTime); err == nil && len(compressed) > 0 {
			dataPoints = compressed
		} else {
			dataPoints = readFiledDataPoints(id, startTime, endTime)
		}
	}

	if downsample > 1 {
		dataPoints = downsampleDataPoints(dataPoints, downsample, aggregation)
	}

	return dataPoints
}

func ReadLastDataPoints(id string, count int) []models.DataPoint {

	if checkIfBufferHasEnoughDataPoints(id, count) {
		return readLastBufferedDataPoints(id, count)
	}

	dataPoints, err := readLastFiledDataPoints(id, count)
	if err != nil {
		utils.Errorln(err)
		return []models.DataPoint{}
	}

	return dataPoints
}

func FlushRemainingDataPoints() {
	SyncAllHandles()
}

func GetAllIds() []string {
	return allIds.Items()
}

// GetKeyCount returns the number of data points for a given key
func GetKeyCount(key string) (int, bool) {
	if cnt, ok := idToCountMap.Load(key); ok {
		return int(cnt.Load()), true
	}
	return 0, false
}

// GetTotalDataPoints returns the approximate total number of data points across all keys.
// This is maintained as an atomic counter for O(1) metrics access.
func GetTotalDataPoints() int64 {
	return totalDataPoints.Load()
}

func GetAllIdsWithCount() []models.KeyCount {
	keys := allIds.Items()

	var keyCount = []models.KeyCount{}
	for _, key := range keys {
		// Open the file (if needed) and stat it, so keys whose handles were
		// evicted from the LRU still report their real count. This feeds the
		// quota reconciler and idswithcount, which must be accurate regardless
		// of LRU state.
		ref, ok := acquireFileHandle(key+".aof", dataFileHandles)
		if !ok {
			keyCount = append(keyCount, models.KeyCount{Key: key, Count: 0})
			continue
		}
		fileStat, err := ref.file.Stat()
		ref.release()
		if err != nil {
			keyCount = append(keyCount, models.KeyCount{Key: key, Count: 0})
			continue
		}
		keyCount = append(keyCount, models.KeyCount{Key: key, Count: int(fileStat.Size() / 16)})
	}

	return keyCount
}

// CompactKey reads all data points for a key and rewrites them to a compacted file.
// This removes gaps left by deleted data points and reduces file size.
func CompactKey(key string) error {
	if key == "" || !allIds.Contains(key) {
		return fmt.Errorf("key not found: %s", key)
	}

	lock, _ := dataPatchLocks.LoadOrStore(key, &sync.Mutex{})
	lock.Lock()
	defer lock.Unlock()

	// Read all existing data points
	dataPoints := readFiledDataPoints(key, 0, math.MaxInt64)
	if dataPoints == nil {
		return nil // empty file, nothing to compact
	}

	if len(dataPoints) == 0 {
		return nil
	}

	// Write to a temporary file first for atomic replacement
	tmpDataFile := utils.DataDir + "/" + key + ".aof.tmp"
	tmpIdxFile := utils.DataDir + "/" + key + ".idx.tmp"

	// Remove any leftover temp files
	os.Remove(tmpDataFile)
	os.Remove(tmpIdxFile)

	// Close existing handles (the eviction callback closes when idle)
	dataFileHandles.Delete(key + ".aof")
	indexFileHandles.Delete(key + ".idx")

	// Create temp data file and write all points
	tmpDataFileHandle, err := os.OpenFile(tmpDataFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp data file: %w", err)
	}

	tmpIdxFileHandle, err := os.OpenFile(tmpIdxFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		tmpDataFileHandle.Close()
		return fmt.Errorf("failed to create temp index file: %w", err)
	}

	// Write data points and rebuild index
	count := int64(0)
	for _, dp := range dataPoints {
		if err := writeRecord(tmpDataFileHandle, dp.Timestamp, dp.Value); err != nil {
			tmpDataFileHandle.Close()
			tmpIdxFileHandle.Close()
			os.Remove(tmpDataFile)
			os.Remove(tmpIdxFile)
			return fmt.Errorf("failed to write compact data: %w", err)
		}
		count++
		if count%indexInterval == 0 {
			offset := (count - 1) * 16
			if err := writeIndexEntry(tmpIdxFileHandle, dp.Timestamp, offset); err != nil {
				tmpDataFileHandle.Close()
				tmpIdxFileHandle.Close()
				os.Remove(tmpDataFile)
				os.Remove(tmpIdxFile)
				return fmt.Errorf("failed to write compact index: %w", err)
			}
		}
	}

	tmpDataFileHandle.Close()
	tmpIdxFileHandle.Close()

	realDataFile := utils.DataDir + "/" + key + ".aof"
	realIdxFile := utils.DataDir + "/" + key + ".idx"

	// Atomically rename: idx first (smaller), then data.
	// If idx rename succeeds but data rename fails, rollback idx.
	// Retries handle transient Windows file locks (AV scanning etc.).
	if err := renameWithRetry(tmpIdxFile, realIdxFile); err != nil {
		os.Remove(tmpDataFile)
		os.Remove(tmpIdxFile)
		return fmt.Errorf("failed to rename index file: %w", err)
	}
	if err := renameWithRetry(tmpDataFile, realDataFile); err != nil {
		// Rollback: restore old idx from what was just renamed
		_ = os.Rename(realIdxFile, tmpIdxFile)
		os.Remove(tmpDataFile)
		return fmt.Errorf("failed to rename data file: %w", err)
	}

	// Re-open file handles and update caches
	if ref, ok := acquireFileHandle(key+".aof", dataFileHandles); ok {
		ref.release()
	}
	if ref, ok := acquireFileHandle(key+".idx", indexFileHandles); ok {
		ref.release()
	}

	// Write Gorilla-compressed version if enabled
	if utils.CompactionCompression {
		if err := writeCompressedWAL(key, dataPoints); err != nil {
			utils.Error("Failed to write compressed WAL for %s: %v", key, err)
		}
	}

	if len(dataPoints) > 0 {
		lastValue.Store(key, dataPoints[len(dataPoints)-1].Value)
		lastTimestamp.Store(key, dataPoints[len(dataPoints)-1].Timestamp)
	}

	// Update count map, adjusting global counter for the difference
	if oldCnt, ok := idToCountMap.Load(key); ok {
		totalDataPoints.Add(-oldCnt.Load())
	}
	newCount := &atomic.Int64{}
	newCount.Store(count)
	idToCountMap.Store(key, newCount)
	totalDataPoints.Add(count)

	utils.Log("Compacted key %s: %d points, %d records", key, len(dataPoints), count)
	return nil
}

// renameWithRetry renames a file, retrying briefly on transient failures.
// On Windows, a rename can fail with "Access is denied" right after the
// destination was closed (e.g. antivirus scanning); a short retry avoids
// spurious compaction failures.
func renameWithRetry(oldPath, newPath string) error {
	const attempts = 5
	const delay = 20 * time.Millisecond
	var err error
	for i := 0; i < attempts; i++ {
		err = os.Rename(oldPath, newPath)
		if err == nil {
			return nil
		}
		time.Sleep(delay)
	}
	return err
}
