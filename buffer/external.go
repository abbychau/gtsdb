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
			if id != "" {
				allIds.Add(id)
			}
		}
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

	dfk := dataPointId + ".aof"
	ifk := dataPointId + ".idx"

	dataFileHandles.Delete(dfk)
	indexFileHandles.Delete(ifk)
	allIds.Remove(dataPointId)

	newDfk := newId + ".aof"
	newIfk := newId + ".idx"

	//rename the file
	os.Rename(utils.DataDir+"/"+dfk, utils.DataDir+"/"+newDfk)
	os.Rename(utils.DataDir+"/"+ifk, utils.DataDir+"/"+newIfk)

	prepareFileHandles(newDfk, dataFileHandles)
	prepareFileHandles(newIfk, indexFileHandles)

	allIds.Add(newId)

	renameLock.Unlock()
}
func DeleteKey(dataPointId string) {
	utils.Log("Deleting key: %v", dataPointId)
	renameLock.Lock()
	dfk := dataPointId + ".aof"
	ifk := dataPointId + ".idx"

	//close file handles
	dfh, _ := dataFileHandles.Load(dfk)
	dfh.Close()
	ifh, _ := indexFileHandles.Load(ifk)
	ifh.Close()

	dataFileHandles.Delete(dfk)
	indexFileHandles.Delete(ifk)
	allIds.Remove(dataPointId)

	//delete the file
	err := os.Remove(utils.DataDir + "/" + dfk)
	if err != nil {
		utils.Errorln(err)
	}
	err = os.Remove(utils.DataDir + "/" + ifk)
	if err != nil {
		utils.Errorln(err)
	}
	renameLock.Unlock()
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

func GetAllIdsWithCount() map[string]int {
	keys := allIds.Items()

	idCount := make(map[string]int)
	for _, key := range keys {
		fh := prepareFileHandles(key+".aof", dataFileHandles)
		fileStat, _ := fh.Stat()
		idCount[key] = int(fileStat.Size() / 16)
	}

	return idCount
}
