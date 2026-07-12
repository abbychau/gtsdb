package buffer

import (
	"gtsdb/utils"
	"sync"
	"time"
)

var (
	flushTicker   *time.Ticker
	flushStop     chan struct{}
	flushOnce     sync.Once
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

	go func() {
		for {
			select {
			case <-flushTicker.C:
				doFlush()
			case <-flushStop:
				flushTicker.Stop()
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

	flushOnce.Do(func() {
		close(flushStop)
		flusherActive = false
	})
}

// doFlush syncs all open file handles.
func doFlush() {
	SyncAllHandles()
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
		if fh, ok := dataFileHandles.Get(key + ".aof"); ok {
			if err := fh.Sync(); err != nil {
				utils.Error("async-flusher: error syncing data file %s: %v", key, err)
			}
		}
		if fh, ok := indexFileHandles.Get(key + ".idx"); ok {
			if err := fh.Sync(); err != nil {
				utils.Error("async-flusher: error syncing index file %s: %v", key, err)
			}
		}
	}
}
