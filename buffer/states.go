package buffer

import (
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/synchronous"
	"gtsdb/utils"
	"os"
	"sync"
	"sync/atomic"
)

const indexInterval = 5000

var cacheSize = 0

// Limit open file handles with an LRU to avoid exceeding OS limits
// IMPORTANT: maximum number of open file handles per process on many systems is 1024
var dataFileHandles *concurrent.LRU[string, *os.File]
var indexFileHandles *concurrent.LRU[string, *os.File]
var idToRingBufferMap = concurrent.NewMap[string, *synchronous.RingBuffer[models.DataPoint]]()
var idToCountMap = concurrent.NewMap[string, *atomic.Int64]()
var allIds = concurrent.NewSet[string]()

var lastValue = concurrent.NewMap[string, float64]()
var lastTimestamp = concurrent.NewMap[string, int64]()

// mutex
var renameLock sync.Mutex
var dataPatchLocks = concurrent.NewMap[string, *sync.Mutex]()
var fileWriteLocks = concurrent.NewMap[string, *sync.Mutex]()

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
	
	utils.Logln("初始化文件句柄LRU，容量：", capacity)
}
