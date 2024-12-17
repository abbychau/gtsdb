package buffer

import (
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/synchronous"
	"os"
	"sync"
	"sync/atomic"
)

const indexInterval = 5000

var cacheSize = 100

var dataFileHandles = concurrent.NewMap[string, *os.File]()
var indexFileHandles = concurrent.NewMap[string, *os.File]()
var idToRingBufferMap = concurrent.NewMap[string, *synchronous.RingBuffer[models.DataPoint]]()
var idToCountMap = concurrent.NewMap[string, *atomic.Int64]()
var allIds = concurrent.NewSet[string]()

var lastValue = make(map[string]float64)
var lastTimestamp = make(map[string]int64)

// mutex
var renameLock sync.Mutex
