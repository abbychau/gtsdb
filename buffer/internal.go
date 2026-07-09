package buffer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

func writeBinary(file *os.File, data ...interface{}) error {
	for _, d := range data {
		err := binary.Write(file, binary.LittleEndian, d)
		if err != nil {
			return fmt.Errorf("error writing binary to file: %w", err)
		}
	}
	return nil
}

func readBinary(reader io.Reader, data ...interface{}) error {
	for _, d := range data {
		err := binary.Read(reader, binary.LittleEndian, d)
		if err != nil {
			return err
		}
	}
	return nil
}

func storeDataPoints(dataPointId string, dataPoints []models.DataPoint) {
	lock, _ := fileWriteLocks.LoadOrStore(dataPointId, &sync.Mutex{})
	lock.Lock()
	defer lock.Unlock()

	dataFile := prepareFileHandles(dataPointId+".aof", dataFileHandles)
	indexFile := prepareFileHandles(dataPointId+".idx", indexFileHandles)
	if dataFile == nil {
		utils.Error("Cannot open data file for %s, skipping write", dataPointId)
		return
	}
	for _, dataPoint := range dataPoints {
		if err := writeBinary(dataFile, dataPoint.Timestamp, dataPoint.Value); err != nil {
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
	// Sync once after all points are written instead of per-point
	dataFile.Sync()
}

func prepareFileHandles(fileName string, handleMap *concurrent.LRU[string, *os.File]) *os.File {
	if file, ok := handleMap.Get(fileName); ok {
		return file
	}

	fullPath := utils.DataDir + "/" + fileName
	dir := filepath.Dir(fullPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			utils.Error("Error creating directory %s: %v", dir, err)
			return nil
		}
	}

	file, err := os.OpenFile(fullPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		utils.Error("Error opening file %s: %v", fullPath, err)
		return nil
	}
	handleMap.Put(fileName, file)

	if strings.HasSuffix(fileName, ".aof") {
		if _, ok := idToCountMap.Load(fileName[:len(fileName)-4]); !ok {
			fileInfo, err := file.Stat()
			if err != nil {
				utils.Error("Error getting file info for %s: %v", fullPath, err)
				return file
			}
			fileLength := fileInfo.Size()
			count := &atomic.Int64{}
			count.Store(fileLength / 16)
			idToCountMap.Store(fileName[:len(fileName)-4], count)
			totalDataPoints.Add(fileLength / 16)
		}
	}
	return file
}

func readLastFiledDataPoints(id string, count int) ([]models.DataPoint, error) {
	file := prepareFileHandles(id+".aof", dataFileHandles)
	if file == nil {
		return nil, nil
	}

	// Get file size and calculate actual record count
	fileInfo, err := file.Stat()
	if err != nil {
		utils.Error("Error getting file info: %v", err)
		return nil, err
	}
	fileSize := fileInfo.Size()

	// Ensure file size is aligned to 16-byte records
	actualRecordCount := fileSize / 16
	if count > int(actualRecordCount) {
		count = int(actualRecordCount)
	}

	// Calculate proper aligned position from the start of valid records
	alignedFileSize := actualRecordCount * 16
	seekOffset := int64(count * 16)
	seekPosition := alignedFileSize - seekOffset

	_, err = file.Seek(seekPosition, io.SeekStart)
	if err != nil {
		utils.Error("Error seeking to position %d: %v", seekPosition, err)
		file.Seek(0, io.SeekStart)
	}

	reader := bufio.NewReader(file)

	var dataPoints []models.DataPoint
	for {
		var timestamp int64
		var value float64
		err := readBinary(reader, &timestamp, &value)
		if err != nil {
			if err == io.EOF {
				break
			}
			utils.Error("Error reading file: %v", err)
			return nil, err
		}

		dataPoints = append(dataPoints, models.DataPoint{
			Key:       id,
			Timestamp: timestamp,
			Value:     value,
		})
	}

	return dataPoints, nil
}

func updateIndexFile(indexFile *os.File, timestamp int64, offset int64) error {
	return writeBinary(indexFile, timestamp, offset)
}

func readFiledDataPoints(id string, startTime int64, endTime int64) []models.DataPoint {
	file := prepareFileHandles(id+".aof", dataFileHandles)
	if file == nil {
		return nil
	}
	var dataPoints []models.DataPoint
	reader := bufio.NewReader(file)

	indexFilename := id + ".idx"
	indexFileInterface, ok := indexFileHandles.Get(indexFilename)
	if ok {
		indexFile := indexFileInterface
		indexReader := bufio.NewReader(indexFile)
		offset := int64(0)

		_, err := indexFile.Seek(0, io.SeekStart)
		if err != nil {
			utils.Error("Error seeking index file: %v", err)
			return nil
		}

		for {
			var timestamp int64
			var fileOffset int64
			err := readBinary(indexReader, &timestamp, &fileOffset)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					utils.Error("Error reading index file: %v", err)
					return nil
				}
			}

			if timestamp > startTime {
				break
			}
			offset = fileOffset
		}

		_, err = file.Seek(offset, io.SeekStart)
		if err != nil {
			utils.Error("Error seeking data file: %v", err)
			return nil
		}
	} else {
		_, err := file.Seek(0, io.SeekStart)
		if err != nil {
			utils.Error("Error seeking data file: %v", err)
			return nil
		}
	}

	for {
		var timestamp int64
		var value float64
		err := readBinary(reader, &timestamp, &value)
		if err != nil {
			if err == io.EOF {
				break
			}
			utils.Error("Error reading file: %v", err)
			return nil
		}

		if timestamp > endTime {
			break
		}

		if timestamp >= startTime && timestamp <= endTime {
			dataPoints = append(dataPoints, models.DataPoint{
				Key:       id,
				Timestamp: timestamp,
				Value:     value,
			})
		}
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

	var result []models.DataPoint
	for i := 0; i < rb.Size(); i++ {
		dataPoint := rb.Get(i)
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

	if count > rb.Size() {
		count = rb.Size()
	}
	if count == 0 {
		return []models.DataPoint{}
	}

	result := make([]models.DataPoint, count)
	for i := 0; i < count; i++ {
		result[i] = rb.Get(rb.Size() - count + i)
	}
	return result
}

func downsampleDataPoints(dataPoints []models.DataPoint, downsample int, aggregation string) []models.DataPoint {
	if len(dataPoints) == 0 {
		return dataPoints
	}

	// For percentile-based aggregations, we need to collect values per interval
	needsValueCollection := aggregation == "median" || aggregation == "p50" || aggregation == "p95" || aggregation == "p99"

	var downsampled []models.DataPoint
	intervalStart := dataPoints[0].Timestamp
	intervalSum := 0.0
	intervalCount := 0
	intervalMin := dataPoints[0].Value
	intervalMax := dataPoints[0].Value
	intervalFirst := dataPoints[0].Value
	intervalLast := dataPoints[0].Value
	var intervalValues []float64

	flushInterval := func(dp models.DataPoint) {
		if intervalCount == 0 {
			return
		}
		var value float64
		switch aggregation {
		case "avg":
			value = intervalSum / float64(intervalCount)
		case "sum":
			value = intervalSum
		case "min":
			value = intervalMin
		case "max":
			value = intervalMax
		case "first":
			value = intervalFirst
		case "last":
			value = intervalLast
		case "count":
			value = float64(intervalCount)
		case "median", "p50":
			if len(intervalValues) > 0 {
				sort.Float64s(intervalValues)
				value = intervalValues[len(intervalValues)/2]
			}
		case "p95":
			if len(intervalValues) > 0 {
				sort.Float64s(intervalValues)
				idx := int(float64(len(intervalValues)) * 0.95)
				if idx >= len(intervalValues) {
					idx = len(intervalValues) - 1
				}
				value = intervalValues[idx]
			}
		case "p99":
			if len(intervalValues) > 0 {
				sort.Float64s(intervalValues)
				idx := int(float64(len(intervalValues)) * 0.99)
				if idx >= len(intervalValues) {
					idx = len(intervalValues) - 1
				}
				value = intervalValues[idx]
			}
		default:
			value = intervalSum / float64(intervalCount)
		}
		downsampled = append(downsampled, models.DataPoint{
			Key:       dp.Key,
			Timestamp: intervalStart,
			Value:     value,
		})
	}

	resetInterval := func(dp models.DataPoint) {
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
	}

	addToInterval := func(dp models.DataPoint) {
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

	resetInterval(dataPoints[0])

	for _, dp := range dataPoints[1:] {
		if dp.Timestamp-intervalStart >= int64(downsample) {
			flushInterval(dp)
			resetInterval(dp)
		} else {
			addToInterval(dp)
		}
	}

	if intervalCount > 0 {
		flushInterval(dataPoints[len(dataPoints)-1])
	}

	return downsampled
}

// InitFileHandles initializes the file handle LRUs with the configured capacity
func InitFileHandles() {
	capacity := utils.FileHandleLRUCapacity

	dataFileHandles = concurrent.NewLRUWithEvict[string, *os.File](capacity, func(_ string, f *os.File) {
		if f != nil {
			f.Close()
		}
	})

	indexFileHandles = concurrent.NewLRUWithEvict[string, *os.File](capacity, func(_ string, f *os.File) {
		if f != nil {
			f.Close()
		}
	})

	utils.Logln("Handle LRU 容量：", capacity)
}
