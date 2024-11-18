package buffer

import (
	"fmt"
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/synchronous"
	"gtsdb/utils"
	"io"
	"os"
)

func StoreDataPointBuffer(dataPoint models.DataPoint) {
	if maxUnflushedDataPoints == 0 {
		storeDataPoints(dataPoint.ID, []models.DataPoint{dataPoint})
		return
	}

	rb, ok := idToRingBufferMap.Get(dataPoint.ID)
	if !ok {
		rb = synchronous.NewRingBuffer[models.DataPoint](maxUnflushedDataPoints)
		idToRingBufferMap.Set(dataPoint.ID, rb)
	}
	rb.Push(dataPoint)

	storeDataPoints(dataPoint.ID, []models.DataPoint{dataPoint})
	lastValue[dataPoint.ID] = dataPoint.Value
	lastTimestamp[dataPoint.ID] = dataPoint.Timestamp
}

func readBufferedDataPoints(id string, startTime, endTime int64) []models.DataPoint {
	if maxUnflushedDataPoints == 0 {
		return []models.DataPoint{}
	}

	rb, ok := idToRingBufferMap.Get(id)
	if !ok {
		return []models.DataPoint{}
	}

	var result []models.DataPoint
	for i := 0; i < rb.Size(); i++ {
		dataPoint := rb.Get(i)
		if dataPoint.Timestamp >= startTime && dataPoint.Timestamp <= endTime {
			result = append(result, dataPoint)
		}
	}
	return result
}

func readLastBufferedDataPoints(id string, count int) []models.DataPoint {
	if count == 1 && lastTimestamp[id] != 0 {
		return []models.DataPoint{{Timestamp: lastTimestamp[id], Value: lastValue[id]}}
	}

	rb, ok := idToRingBufferMap.Get(id)
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

func storeDataPoints(dataPointId string, dataPoints []models.DataPoint) {
	dataFile := prepareFileHandles(dataPointId+".aof", dataFileHandles)
	metaFile := prepareFileHandles(dataPointId+".meta", metaFileHandles)
	indexFile := prepareFileHandles(dataPointId+".idx", indexFileHandles)
	for _, dataPoint := range dataPoints {

		line := fmt.Sprintf("%d,%.8e\n", dataPoint.Timestamp, dataPoint.Value)
		dataFile.WriteString(line)
		count := readMetaCount(metaFile)
		count++
		writeMetaCount(metaFile, count)

		if count%indexInterval == 0 {

			//end position of this file
			offset, _ := dataFile.Seek(0, io.SeekEnd)
			offset -= int64(len(line))
			updateIndexFile(indexFile, dataPoint.Timestamp, offset)
		}
	}

}
func prepareFileHandles(fileName string, handleArray *concurrent.HashMap[string, *os.File]) *os.File {

	file, ok := handleArray.Get(fileName)
	if !ok {
		var err error
		file, err = os.OpenFile(utils.DataDir+"/"+fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			panic(err)
		}
		handleArray.Set(fileName, file)
	}
	return file
}

func FlushRemainingDataPoints() {

	//fsync all file handles
	for _, file := range dataFileHandles.Values() {
		file.Sync()
	}
	for _, file := range metaFileHandles.Values() {
		file.Sync()
	}
	for _, file := range indexFileHandles.Values() {
		file.Sync()
	}
}
