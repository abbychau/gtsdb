package buffer

import (
	"gtsdb/concurrent"
	"gtsdb/utils"
	"sync"
	"time"
)

var (
	flushTicker   *time.Ticker
	flushStop     chan struct{}
	flusherActive bool
	flusherMu     sync.Mutex
)

// StartAsyncFlusher starts a background goroutine that periodically syncs
// all open data and index file handles to disk.
func StartAsyncFlusher(interval time.Duration) {
	flusherMu.Lock()
	defer flusherMu.Unlock()

	if flusherActive {
		return
	}
	flusherActive = true

	flushStop = make(chan struct{})
	flushTicker = time.NewTicker(interval)

	// Capture locals so the goroutine never reads the mutable globals: a
	// second StartAsyncFlusher (e.g. loadConfig being called again) can
	// reassign them without racing the running goroutine.
	ticker := flushTicker
	stop := flushStop

	go func() {
		for {
			select {
			case <-ticker.C:
				doFlush()
			case <-stop:
				ticker.Stop()
				doFlush() // final flush on stop
				return
			}
		}
	}()

	utils.Log("[async-flusher] Started with interval %v", interval)
}

// StopAsyncFlusher stops the background flusher goroutine.
func StopAsyncFlusher() {
	flusherMu.Lock()
	defer flusherMu.Unlock()

	if !flusherActive {
		return
	}

	// Guarded by flusherMu + flusherActive, so flushStop can never be
	// closed twice (no sync.Once needed; it also allows restart cycles).
	close(flushStop)
	flusherActive = false
}

// doFlush syncs all open file handles.
func doFlush() {
	SyncAllHandles()
}

// syncHandle syncs a single handle if it is present in the LRU. Extracted so
// the acquire/release pairing uses defer (guaranteed on all paths) without
// accumulating deferred releases inside the caller's loop.
func syncHandle(l *concurrent.LRU[string, *refFile], key string) {
	if ref, ok := refFromLRU(l, key); ok {
		defer ref.release()
		if err := ref.file.Sync(); err != nil {
			utils.Error("async-flusher: error syncing file %s: %v", key, err)
		}
	}
}

// SyncAllHandles performs a one-time sync of only file handles that have pending writes.
func SyncAllHandles() {
	if dirtyKeys.Size() == 0 {
		return
	}

	// Snapshot and clear dirty keys so new writes during sync are captured for next tick
	keys := dirtyKeys.Items()
	dirtyKeys.Clear()

	for _, key := range keys {
		syncHandle(dataFileHandles, key+".aof")
		syncHandle(indexFileHandles, key+".idx")
	}
}
