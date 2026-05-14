package buffer

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
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
)

func InitIDSet() {
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
	prepareFileHandles(dataPointId+".aof", dataFileHandles)
	prepareFileHandles(dataPointId+".idx", indexFileHandles)
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

	// Close and remove old file handles
	if dfh, ok := dataFileHandles.Get(dfk); ok {
		dfh.Close()
		dataFileHandles.Delete(dfk)
	}
	if ifh, ok := indexFileHandles.Get(ifk); ok {
		ifh.Close()
		indexFileHandles.Delete(ifk)
	}

	// Remove from allIds before renaming
	allIds.Remove(dataPointId)

	// Rename the files
	err1 := os.Rename(utils.DataDir+"/"+dfk, utils.DataDir+"/"+newDfk)
	err2 := os.Rename(utils.DataDir+"/"+ifk, utils.DataDir+"/"+newIfk)

	if err1 != nil || err2 != nil {
		utils.Errorln("Error renaming files:", err1, err2)
		return
	}

	// Create new file handles
	prepareFileHandles(newDfk, dataFileHandles)
	prepareFileHandles(newIfk, indexFileHandles)

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

	// close file handles if they are open
	if dfh, ok := dataFileHandles.Get(dfk); ok && dfh != nil {
		dfh.Close()
	}
	if ifh, ok := indexFileHandles.Get(ifk); ok && ifh != nil {
		ifh.Close()
	}

	dataFileHandles.Delete(dfk)
	indexFileHandles.Delete(ifk)
	idToRingBufferMap.Delete(dataPointId)
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

	if dfh, ok := dataFileHandles.Get(dfk); ok && dfh != nil {
		dfh.Close()
	}
	if ifh, ok := indexFileHandles.Get(ifk); ok && ifh != nil {
		ifh.Close()
	}

	dataFileHandles.Delete(dfk)
	indexFileHandles.Delete(ifk)
	idToRingBufferMap.Delete(dataPointId)
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

	prepareFileHandles(dfk, dataFileHandles)
	if _, err := os.Stat(utils.DataDir + "/" + ifk); err == nil {
		prepareFileHandles(ifk, indexFileHandles)
	}
	allIds.Add(dataPointId)

	return true
}

func StoreDataPointBuffer(dataPoint models.DataPoint) {
	allIds.Add(dataPoint.Key)

	if cacheSize == 0 {
		storeDataPoints(dataPoint.Key, []models.DataPoint{dataPoint})
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
	dataFile := prepareFileHandles(key+".aof", dataFileHandles)
	indexFile := prepareFileHandles(key+".idx", indexFileHandles)

	startOffset := int64(0)
	if _, err := indexFile.Seek(0, io.SeekStart); err == nil {
		indexReader := bufio.NewReader(indexFile)
		for {
			var ts int64
			var off int64
			err := readBinary(indexReader, &ts, &off)
			if err != nil {
				break
			}
			if ts > point.Timestamp {
				break
			}
			startOffset = off
		}
	}

	if _, err := dataFile.Seek(startOffset, io.SeekStart); err != nil {
		return false
	}

	reader := bufio.NewReader(dataFile)
	offset := startOffset
	for {
		var ts int64
		var val float64
		err := readBinary(reader, &ts, &val)
		if err != nil {
			if err == io.EOF {
				return false
			}
			return false
		}

		if ts == point.Timestamp {
			// timestamp(int64)=8 bytes; value starts at +8
			if _, err := dataFile.Seek(offset+int64(binary.Size(ts)), io.SeekStart); err != nil {
				return false
			}
			writeBinary(dataFile, point.Value)
			dataFile.Sync()

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
		dataPoints = readFiledDataPoints(id, startTime, endTime)
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

	//fsync all file handles
	dataFileHandles.Range(func(key string, value *os.File) bool {
		value.Sync()
		return true
	})
	indexFileHandles.Range(func(key string, value *os.File) bool {
		value.Sync()
		return true
	})
}

func FormatDataPoints(dataPoints []models.DataPoint) string {
	var response string

	for i, dp := range dataPoints {
		response += fmt.Sprintf("%s,%d,%.2f", dp.Key, dp.Timestamp, dp.Value)
		if i < len(dataPoints)-1 {
			response += "|"
		}
	}

	response += "\n"

	return response
}

// JsonFormatDataPoints
func JsonFormatDataPoints(dataPoints []models.DataPoint) string {
	var response string
	//use json marshal to format the data points
	bytes, _ := json.Marshal(dataPoints)
	response = string(bytes)
	return response
}

func GetAllIds() []string {
	return allIds.Items()
}

func GetAllIdsWithCount() []models.KeyCount {
	keys := allIds.Items()

	var keyCount = []models.KeyCount{}
	for _, key := range keys {
		fh := prepareFileHandles(key+".aof", dataFileHandles)
		fileStat, _ := fh.Stat()
		size := int(fileStat.Size() / 16)
		keyCount = append(keyCount, models.KeyCount{Key: key, Count: size})
	}

	return keyCount
}
