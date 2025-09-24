package buffer

import (
	"fmt"
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/utils"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestLRUFileHandleLimitWith2000Sensors(t *testing.T) {
	// Save original state
	originalDataDir := utils.DataDir
	originalDataFileHandles := dataFileHandles
	originalIndexFileHandles := indexFileHandles
	originalAllIds := allIds
	
	// Setup test environment
	testDir := "/tmp/gtsdb_test_lru_" + fmt.Sprintf("%d", time.Now().UnixNano())
	utils.DataDir = testDir
	
	// Create test directory
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	
	// Initialize fresh state for test
	dataFileHandles = concurrent.NewLRUWithEvict[string, *os.File](utils.FileHandleLRUCapacity, func(_ string, f *os.File) {
		if f != nil {
			f.Close()
		}
	})
	indexFileHandles = concurrent.NewLRUWithEvict[string, *os.File](utils.FileHandleLRUCapacity, func(_ string, f *os.File) {
		if f != nil {
			f.Close()
		}
	})
	allIds = concurrent.NewSet[string]()
	
	// Cleanup function
	defer func() {
		os.RemoveAll(testDir)
		utils.DataDir = originalDataDir
		dataFileHandles = originalDataFileHandles
		indexFileHandles = originalIndexFileHandles
		allIds = originalAllIds
	}()
	
	const numSensors = 2000
	const numDataPointsPerSensor = 5
	
	t.Logf("Creating %d sensors (exceeds LRU capacity of %d)", numSensors, utils.FileHandleLRUCapacity)
	
	// Track initial file descriptor count
	var initialFDs int
	if runtime.GOOS == "linux" {
		initialFDs = countOpenFileDescriptors()
	}
	
	// Create sensors and store data points
	for i := 0; i < numSensors; i++ {
		sensorId := fmt.Sprintf("sensor_%04d", i)
		
		// Store multiple data points for each sensor
		for j := 0; j < numDataPointsPerSensor; j++ {
			dataPoint := models.DataPoint{
				Key:       sensorId,
				Timestamp: time.Now().Unix() + int64(j),
				Value:     float64(i*100 + j),
			}
			StoreDataPointBuffer(dataPoint)
		}
		
		// Periodically check that we haven't exceeded reasonable file handle limits
		if (i+1)%100 == 0 {
			dataHandleCount := dataFileHandles.Len()
			indexHandleCount := indexFileHandles.Len()
			
			t.Logf("After %d sensors: data handles = %d, index handles = %d", 
				i+1, dataHandleCount, indexHandleCount)
			
			// Verify LRU is working - we shouldn't have more handles than capacity
			if dataHandleCount > utils.FileHandleLRUCapacity {
				t.Errorf("Data file handles (%d) exceeded LRU capacity (%d)", 
					dataHandleCount, utils.FileHandleLRUCapacity)
			}
			if indexHandleCount > utils.FileHandleLRUCapacity {
				t.Errorf("Index file handles (%d) exceeded LRU capacity (%d)", 
					indexHandleCount, utils.FileHandleLRUCapacity)
			}
		}
	}
	
	// Final verification
	finalDataHandles := dataFileHandles.Len()
	finalIndexHandles := indexFileHandles.Len()
	totalIds := len(allIds.Items())
	
	t.Logf("Final state: %d sensors created, %d data handles, %d index handles", 
		totalIds, finalDataHandles, finalIndexHandles)
	
	// Verify we created all sensors
	if totalIds != numSensors {
		t.Errorf("Expected %d sensors, but got %d", numSensors, totalIds)
	}
	
	// Verify LRU kept handle counts reasonable
	if finalDataHandles > utils.FileHandleLRUCapacity {
		t.Errorf("Final data handles (%d) exceeded capacity (%d)", 
			finalDataHandles, utils.FileHandleLRUCapacity)
	}
	if finalIndexHandles > utils.FileHandleLRUCapacity {
		t.Errorf("Final index handles (%d) exceeded capacity (%d)", 
			finalIndexHandles, utils.FileHandleLRUCapacity)
	}
	
	// Test that we can still read data from early sensors (should reopen files)
	firstSensor := "sensor_0000"
	lastSensor := fmt.Sprintf("sensor_%04d", numSensors-1)
	
	// Read from first sensor (likely evicted from cache)
	firstData := ReadLastDataPoints(firstSensor, numDataPointsPerSensor)
	if len(firstData) != numDataPointsPerSensor {
		t.Errorf("Expected %d points from first sensor, got %d", 
			numDataPointsPerSensor, len(firstData))
	}
	
	// Read from last sensor (likely in cache)
	lastData := ReadLastDataPoints(lastSensor, numDataPointsPerSensor)
	if len(lastData) != numDataPointsPerSensor {
		t.Errorf("Expected %d points from last sensor, got %d", 
			numDataPointsPerSensor, len(lastData))
	}
	
	// Check file descriptor usage on Linux
	if runtime.GOOS == "linux" {
		finalFDs := countOpenFileDescriptors()
		fdIncrease := finalFDs - initialFDs
		t.Logf("File descriptor increase: %d (initial: %d, final: %d)", 
			fdIncrease, initialFDs, finalFDs)
		
		// We should not have leaked a massive number of file descriptors
		// Allow for some increase but it shouldn't be close to numSensors*2
		maxAllowedIncrease := utils.FileHandleLRUCapacity * 3 // generous buffer
		if fdIncrease > maxAllowedIncrease {
			t.Errorf("Excessive file descriptor increase: %d (max allowed: %d)", 
				fdIncrease, maxAllowedIncrease)
		}
	}
}

// countOpenFileDescriptors counts the number of open file descriptors on Linux
func countOpenFileDescriptors() int {
	if runtime.GOOS != "linux" {
		return 0
	}
	
	pid := os.Getpid()
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	
	files, err := os.ReadDir(fdDir)
	if err != nil {
		return 0
	}
	
	return len(files)
}

func TestLRUEvictionCallback(t *testing.T) {
	// Save original state
	originalDataDir := utils.DataDir
	originalDataFileHandles := dataFileHandles
	
	// Setup test environment  
	testDir := "/tmp/gtsdb_test_eviction_" + fmt.Sprintf("%d", time.Now().UnixNano())
	utils.DataDir = testDir
	
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	
	// Create a small LRU cache for testing eviction
	evictedFiles := make([]*os.File, 0)
	testCapacity := 3
	
	dataFileHandles = concurrent.NewLRUWithEvict[string, *os.File](testCapacity, func(_ string, f *os.File) {
		if f != nil {
			evictedFiles = append(evictedFiles, f)
			f.Close()
		}
	})
	
	defer func() {
		os.RemoveAll(testDir)
		utils.DataDir = originalDataDir
		dataFileHandles = originalDataFileHandles
	}()
	
	// Create more files than capacity to trigger eviction
	numFiles := 5
	for i := 0; i < numFiles; i++ {
		sensorId := fmt.Sprintf("eviction_test_%d", i)
		dataPoint := models.DataPoint{
			Key:       sensorId,
			Timestamp: time.Now().Unix(),
			Value:     float64(i),
		}
		StoreDataPointBuffer(dataPoint)
	}
	
	// Verify eviction happened
	if len(evictedFiles) == 0 {
		t.Error("Expected some files to be evicted, but none were")
	}
	
	expectedEvictions := numFiles - testCapacity
	if len(evictedFiles) < expectedEvictions {
		t.Errorf("Expected at least %d evictions, got %d", expectedEvictions, len(evictedFiles))
	}
	
	// Verify cache size is within limits
	currentSize := dataFileHandles.Len()
	if currentSize > testCapacity {
		t.Errorf("Cache size (%d) exceeded capacity (%d)", currentSize, testCapacity)
	}
	
	t.Logf("Successfully evicted %d files, cache size: %d", len(evictedFiles), currentSize)
}