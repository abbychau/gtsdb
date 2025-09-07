package buffer

import (
	"bufio"
	"encoding/binary"
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/utils"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

func writeBinary(file *os.File, data ...interface{}) {
	for _, d := range data {
		err := binary.Write(file, binary.LittleEndian, d)
		if err != nil {
			utils.Panic(err)
		}
	}
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
	for _, dataPoint := range dataPoints {
		writeBinary(dataFile, dataPoint.Timestamp, dataPoint.Value)
		dataFile.Sync() // Force immediate write to disk

		countValue, _ := idToCountMap.Load(dataPointId)
		count := countValue
		count.Add(1)

		if count.Load()%indexInterval == 0 {
			offset, _ := dataFile.Seek(0, io.SeekEnd)
			offset -= int64(binary.Size(dataPoint.Timestamp) + binary.Size(dataPoint.Value))
			updateIndexFile(indexFile, dataPoint.Timestamp, offset)
		}
	}
}

func prepareFileHandles(fileName string, handleMap *concurrent.Map[string, *os.File]) *os.File {
	fileInterface, ok := handleMap.Load(fileName)
	if !ok {
		var err error
		file, err := os.OpenFile(utils.DataDir+"/"+fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			utils.Panic(err)
		}
		handleMap.Store(fileName, file)

		if strings.HasSuffix(fileName, ".aof") {
			_, ok := idToCountMap.Load(fileName[:len(fileName)-4])
			if !ok {
				fileInfo, err := file.Stat()
				if err != nil {
					utils.Panic(err)
				}
				fileLength := fileInfo.Size()
				count := &atomic.Int64{}
				count.Store(fileLength / 16)
				idToCountMap.Store(fileName[:len(fileName)-4], count)
			}
		}
		return file
	}
	return fileInterface
}

func readLastFiledDataPoints(id string, count int) ([]models.DataPoint, error) {
	file := prepareFileHandles(id+".aof", dataFileHandles)

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

func updateIndexFile(indexFile *os.File, timestamp int64, offset int64) {
	writeBinary(indexFile, timestamp, offset)
}

func readFiledDataPoints(id string, startTime int64, endTime int64) []models.DataPoint {
	file := prepareFileHandles(id+".aof", dataFileHandles)
	var dataPoints []models.DataPoint
	reader := bufio.NewReader(file)

	indexFilename := id + ".idx"
	indexFileInterface, ok := indexFileHandles.Load(indexFilename)
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

	var downsampled []models.DataPoint
	intervalStart := dataPoints[0].Timestamp
	intervalSum := 0.0
	intervalCount := 0
	intervalMin := dataPoints[0].Value
	intervalMax := dataPoints[0].Value
	intervalFirst := dataPoints[0].Value
	intervalLast := dataPoints[0].Value

	for _, dp := range dataPoints {
		if dp.Timestamp-intervalStart >= int64(downsample) {
			if intervalCount > 0 {
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
				default:
					value = intervalSum / float64(intervalCount)
				}
				downsampled = append(downsampled, models.DataPoint{
					Key:       dp.Key,
					Timestamp: intervalStart,
					Value:     value,
				})
			}
			intervalStart = dp.Timestamp
			intervalSum = dp.Value
			intervalCount = 1
			intervalMin = dp.Value
			intervalMax = dp.Value
			intervalFirst = dp.Value
			intervalLast = dp.Value
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
		}
	}

	if intervalCount > 0 {
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
		default:
			value = intervalSum / float64(intervalCount)
		}
		downsampled = append(downsampled, models.DataPoint{
			Key:       dataPoints[len(dataPoints)-1].Key,
			Timestamp: intervalStart,
			Value:     value,
		})
	}

	return downsampled
}
