package buffer

import (
	"encoding/json"
	"fmt"
	"gtsdb/models"
	"gtsdb/synchronous"
	"gtsdb/utils"
	"os"
	"strings"
)

func InitIDSet() {
	// Read all the files in the data directory
	files, err := os.ReadDir(utils.DataDir)
	if err != nil {
		utils.InitDataDirectory()
		return
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".aof") {
			id := file.Name()[:len(file.Name())-4]
			allIds.Add(id)
		}
	}
}

func StoreDataPointBuffer(dataPoint models.DataPoint) {
	allIds.Add(dataPoint.ID)
	if cacheSize == 0 {
		storeDataPoints(dataPoint.ID, []models.DataPoint{dataPoint})
		return
	}

	rb, ok := idToRingBufferMap.Load(dataPoint.ID)
	if !ok {
		newRb := synchronous.NewRingBuffer[models.DataPoint](cacheSize)
		idToRingBufferMap.Store(dataPoint.ID, newRb)
		rb = newRb
	}
	rb.Push(dataPoint)

	storeDataPoints(dataPoint.ID, []models.DataPoint{dataPoint})

	lastValue[dataPoint.ID] = dataPoint.Value
	lastTimestamp[dataPoint.ID] = dataPoint.Timestamp
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
		response += fmt.Sprintf("%s,%d,%.2f", dp.ID, dp.Timestamp, dp.Value)
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
