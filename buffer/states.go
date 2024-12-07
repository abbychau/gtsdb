package buffer

import (
	"sync"
)

const indexInterval = 5000

var cacheSize = 100

var dataFileHandles = &sync.Map{}
var indexFileHandles = &sync.Map{}
var idToRingBufferMap = &sync.Map{}
var idToCountMap = &sync.Map{}

var lastValue = make(map[string]float64)
var lastTimestamp = make(map[string]int64)
