package buffer

import (
	"fmt"
	"gtsdb/concurrent"
	models "gtsdb/models"
	"gtsdb/utils"
	"io"
	"os"
)

const indexInterval = 5000

const maxUnflushedDataPoints = 0

var dataFileHandles = concurrent.NewHashMap[string, *os.File]()
var indexFileHandles = concurrent.NewHashMap[string, *os.File]()
var metaFileHandles = concurrent.NewHashMap[string, *os.File]()
var ringBuffer = concurrent.NewHashMap[string, []models.DataPoint]()

var lastValue = make(map[string]float64)
var lastTimestamp = make(map[string]int64)
var fileIdToBySynced = concurrent.NewSet[string]()

func StoreDataPointBuffer(dataPoint models.DataPoint) {

	if maxUnflushedDataPoints == 0 {
		storeDataPoints(dataPoint.ID, []models.DataPoint{dataPoint})
		return
	}

	var dataPointBuffer = ringBuffer.AssertGet(dataPoint.ID)
	ringBuffer.Set(dataPoint.ID, append(dataPointBuffer, dataPoint))

	if len(dataPointBuffer) >= maxUnflushedDataPoints {
		go storeDataPointBufferToFile(dataPoint.ID)
	}
	lastValue[dataPoint.ID] = dataPoint.Value
	lastTimestamp[dataPoint.ID] = dataPoint.Timestamp
}

func readBufferedDataPoints(id string, startTime, endTime int64) []models.DataPoint {

	if maxUnflushedDataPoints == 0 {
		return []models.DataPoint{}
	}

	var result []models.DataPoint
	for _, dataPoint := range ringBuffer.AssertGet(id) {
		if dataPoint.Timestamp >= startTime && dataPoint.Timestamp <= endTime {
			result = append(result, dataPoint)
		}
	}

	return result
}
func readLastBufferedDataPoints(id string, count int) []models.DataPoint {

	if count == 1 {
		return []models.DataPoint{{Timestamp: lastTimestamp[id], Value: lastValue[id]}}
	}
	var dataPointBuffer = ringBuffer.AssertGet(id)
	if count > len(dataPointBuffer) {
		count = len(dataPointBuffer)
	}
	return dataPointBuffer[len(dataPointBuffer)-count:]
}

func storeDataPointBufferToFile(id string) {

	dataPoints := ringBuffer.AssertGet(id)
	ringBuffer.Delete(id)
	storeDataPoints(id, dataPoints)
}

func FlushRemainingDataPoints() {
	ringBuffer.ForEach(func(id string, dataPoints []models.DataPoint) {
		storeDataPointBufferToFile(id)
	})
}

func storeDataPoints(dataPointId string, dataPoints []models.DataPoint) {
	dataFile := prepareFileHandles(dataPointId+".aof", dataFileHandles)
	metaFile := prepareFileHandles(dataPointId+".meta", metaFileHandles)
	indexFile := prepareFileHandles(dataPointId+".idx", indexFileHandles)
	for _, dataPoint := range dataPoints {

		line := fmt.Sprintf("%d,%.2f\n", dataPoint.Timestamp, dataPoint.Value)
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
	fileIdToBySynced.Add(dataPointId)

}
func prepareFileHandles(fileName string, handleArray *concurrent.HashMap[string, *os.File]) *os.File {

	file, ok := handleArray.Get(fileName)
	if !ok {
		var err error
		file, err = os.OpenFile(utils.DataDir+"/"+fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)
		if err != nil {
			panic(err)
		}
		handleArray.Set(fileName, file)
	}
	return file
}
