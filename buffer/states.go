package buffer

import (
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/synchronous"
	"os"
)

const indexInterval = 5000

var cacheSize = 100

var dataFileHandles = concurrent.NewHashMap[string, *os.File]()
var indexFileHandles = concurrent.NewHashMap[string, *os.File]()
var metaFileHandles = concurrent.NewHashMap[string, *os.File]()
var idToRingBufferMap = concurrent.NewHashMap[string, *synchronous.RingBuffer[models.DataPoint]]()

var lastValue = make(map[string]float64)
var lastTimestamp = make(map[string]int64)
