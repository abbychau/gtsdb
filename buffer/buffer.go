package buffer

import (
	"fmt"
	models "gtsdb/models"
	"gtsdb/utils"
	"io"
	"os"
	"sync"
	"time"
)

const indexInterval = 5000

const maxUnflushedDataPoints = 5000

var dataFileHandles = make(map[string]*os.File)
var indexFileHandles = make(map[string]*os.File)
var metaFileHandles = make(map[string]*os.File)
var ringBuffer = make(map[string][]models.DataPoint)
var rwMutex = make(map[string]*sync.RWMutex)
var lockForInit = &sync.Mutex{}
var lastValue = make(map[string]float64)
var lastTimestamp = make(map[string]int64)

func StartPeriodicFlushWorker() {
	for {
		time.Sleep(5 * time.Second)
		FlushRemainingDataPoints()
	}
}

func initLock(id string) {
	lockForInit.Lock()
	defer lockForInit.Unlock()
	if _, ok := rwMutex[id]; !ok {
		rwMutex[id] = &sync.RWMutex{}
	}
}

func StoreDataPointBuffer(dataPoint models.DataPoint) {
	initLock(dataPoint.ID)
	rwMutex[dataPoint.ID].Lock()
	defer rwMutex[dataPoint.ID].Unlock()
	ringBuffer[dataPoint.ID] = append(ringBuffer[dataPoint.ID], dataPoint)
	if len(ringBuffer[dataPoint.ID]) >= maxUnflushedDataPoints {
		go storeDataPointBufferToFile(dataPoint.ID)
	}
	lastValue[dataPoint.ID] = dataPoint.Value
	lastTimestamp[dataPoint.ID] = dataPoint.Timestamp
}

func readBufferedDataPoints(id string, startTime, endTime int64) []models.DataPoint {
	initLock(id)
	rwMutex[id].RLock()
	defer rwMutex[id].RUnlock()
	var result []models.DataPoint
	for _, dataPoint := range ringBuffer[id] {
		if dataPoint.Timestamp >= startTime && dataPoint.Timestamp <= endTime {
			result = append(result, dataPoint)
		}
	}

	return result
}
func readLastBufferedDataPoints(id string, count int) []models.DataPoint {
	initLock(id)
	rwMutex[id].RLock()
	defer rwMutex[id].RUnlock()

	if count == 1 {
		return []models.DataPoint{{Timestamp: lastTimestamp[id], Value: lastValue[id]}}
	}

	if count > len(ringBuffer[id]) {
		count = len(ringBuffer[id])
	}
	return ringBuffer[id][len(ringBuffer[id])-count:]
}

func storeDataPointBufferToFile(id string) {
	initLock(id)
	rwMutex[id].Lock()
	defer rwMutex[id].Unlock()
	dataPoints := ringBuffer[id]
	ringBuffer[id] = nil
	storeDataPoints(id, dataPoints)
}

func FlushRemainingDataPoints() {
	for id := range ringBuffer {
		storeDataPointBufferToFile(id)
	}
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
}
func prepareFileHandles(fileName string, handleArray map[string]*os.File) *os.File {
	initLock(fileName)
	rwMutex[fileName].Lock()
	defer rwMutex[fileName].Unlock()
	file, ok := handleArray[fileName]
	if !ok {
		var err error
		file, err = os.OpenFile(utils.DataDir+"/"+fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		handleArray[fileName] = file
	}
	return file
}
