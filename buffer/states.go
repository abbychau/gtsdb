package buffer

import (
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/synchronous"
	"os"
	"sync/atomic"
)

const indexInterval = 5000

var cacheSize = 100

var dataFileHandles = concurrent.NewHashMap[string, *os.File]()
var indexFileHandles = concurrent.NewHashMap[string, *os.File]()
var idToRingBufferMap = concurrent.NewHashMap[string, *synchronous.RingBuffer[models.DataPoint]]()
var idToCountMap = concurrent.NewHashMap[string, *atomic.Int64]()

var lastValue = make(map[string]float64)
var lastTimestamp = make(map[string]int64)
