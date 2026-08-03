package buffer

import (
	"gtsdb/models"
	"gtsdb/utils"
	"os"
	"testing"
	"time"
)

func cleanup() {
	utils.DataDir = "../testdata"
	// create folder if not exists
	if _, err := os.Stat(utils.DataDir); os.IsNotExist(err) {
		_ = os.Mkdir(utils.DataDir, 0755)
	}
	files, _ := os.ReadDir(utils.DataDir)
	for _, file := range files {
		os.Remove(utils.DataDir + "/" + file.Name())
	}

	// Re-initialize file handles to clear any cached state between tests
	InitFileHandles()
}

func TestStoreAndReadDataPoints(t *testing.T) {
	cleanup()
	defer cleanup()

	// Test data
	dataPoint := models.DataPoint{
		Key:       "TestStoreAndReadDataPoints",
		Timestamp: time.Now().Unix(),
		Value:     42.5,
	}

	// Test storing
	StoreDataPointBuffer(dataPoint)

	// Test reading last point
	points := ReadLastDataPoints("TestStoreAndReadDataPoints", 1)
	if len(points) != 1 {
		t.Errorf("Expected 1 point, got %d", len(points))
	}
	if points[0].Value != dataPoint.Value {
		t.Errorf("Expected value %f, got %f", dataPoint.Value, points[0].Value)
	}
}

func TestReadDataPointsWithDownsampling(t *testing.T) {
	cleanup()
	defer cleanup()

	// Store test data points
	now := time.Now().Unix()
	points := []models.DataPoint{
		{Key: "test2", Timestamp: now, Value: 1.0},
		{Key: "test2", Timestamp: now + 1, Value: 2.0},
		{Key: "test2", Timestamp: now + 2, Value: 3.0},
		{Key: "test2", Timestamp: now + 3, Value: 4.0},
	}

	for _, p := range points {
		StoreDataPointBuffer(p)
	}

	// Test reading with downsampling
	result := ReadDataPoints("test2", now, now+3, 2, "avg")
	if len(result) != 2 {
		t.Errorf("Expected 2 downsampled points, got %d", len(result))
	}

	// Test different aggregation methods
	aggMethods := []string{"avg", "sum", "min", "max", "first", "last"}
	for _, method := range aggMethods {
		result := ReadDataPoints("test2", now, now+3, 2, method)
		if len(result) == 0 {
			t.Errorf("No results returned for aggregation method %s", method)
		}
	}
}

func TestFlushRemainingDataPoints(t *testing.T) {
	cleanup()
	defer cleanup()

	// Store some data
	dataPoint := models.DataPoint{
		Key:       "test3",
		Timestamp: time.Now().Unix(),
		Value:     42.5,
	}
	StoreDataPointBuffer(dataPoint)

	// Test flush
	FlushRemainingDataPoints()

	// Verify data can still be read after flush
	points := ReadLastDataPoints("test3", 1)
	if len(points) != 1 {
		t.Errorf("Expected 1 point after flush, got %d", len(points))
	}
}

func TestReadLastDataPoints(t *testing.T) {
	cleanup()
	defer cleanup()

	// Test data - generate 6000 points
	now := time.Now().Unix()
	points := make([]models.DataPoint, 6000)
	for i := 0; i < 6000; i++ {
		points[i] = models.DataPoint{
			Key:       "TestReadLast",
			Timestamp: now + int64(i),
			Value:     float64(i),
		}
	}

	// Store test data points
	for _, p := range points {
		StoreDataPointBuffer(p)
	}

	// Test reading all 6000 points
	result := ReadLastDataPoints("TestReadLast", 6000)
	if len(result) != 6000 {
		t.Errorf("Expected 6000 points, got %d", len(result))
	}

	// Verify data continuity
	valueMap := make(map[float64]bool)
	for _, p := range result {
		valueMap[p.Value] = true
	}

	// Check if all values are present
	for i := 0; i < 6000; i++ {
		if !valueMap[float64(i)] {
			t.Errorf("Missing value %d in result set", i)
		}
	}
}

func TestInitIDSet(t *testing.T) {
	cleanup()
	defer cleanup()

	// Create some test files
	testFiles := []string{"test1.aof", "test2.aof", "test3.aof"}
	for _, fname := range testFiles {
		f, _ := os.Create(utils.DataDir + "/" + fname)
		f.Close()
	}

	// Initialize ID set
	InitIDSet()

	// Get all IDs
	ids := GetAllIds()

	// Verify all test IDs are present
	expectedIds := []string{"test1", "test2", "test3"}

	// Check if each expected ID exists
	for _, expectedId := range expectedIds {
		found := false
		for _, id := range ids {
			if id == expectedId {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected ID %s not found in result", expectedId)
		}
	}
}

func TestReadDataPointsEmptyResult(t *testing.T) {
	cleanup()
	defer cleanup()
	utils.DataDir = "../testdata"
	InitIDSet()

	// Test reading non-existent ID
	result := ReadDataPoints("nonexistent", 0, 1000, 1, "avg")
	if len(result) != 0 {
		t.Errorf("Expected empty result for non-existent ID, got %d points", len(result))
	}

	// Test reading with invalid time range
	dataPoint := models.DataPoint{
		Key:       "test_empty",
		Timestamp: 1000,
		Value:     42.5,
	}
	StoreDataPointBuffer(dataPoint)

	result = ReadDataPoints("test_empty", 2000, 3000, 1, "avg")
	if len(result) != 0 {
		t.Errorf("Expected empty result for invalid time range, got %d points", len(result))
	}
}

func TestStoreDataPointBufferExceedingCacheSize(t *testing.T) {
	cleanup()
	defer cleanup()

	// Set small cache size
	cacheSize = 3

	// Test data - more points than cache size
	now := time.Now().Unix()
	points := []models.DataPoint{
		{Key: "TestCacheOverflow", Timestamp: now, Value: 1.0},
		{Key: "TestCacheOverflow", Timestamp: now + 1, Value: 2.0},
		{Key: "TestCacheOverflow", Timestamp: now + 2, Value: 3.0},
		{Key: "TestCacheOverflow", Timestamp: now + 3, Value: 4.0},
		{Key: "TestCacheOverflow", Timestamp: now + 4, Value: 5.0},
	}

	// Store points
	for _, point := range points {
		StoreDataPointBuffer(point)
	}

	// Test 1: Verify ring buffer respects cache size
	rb, ok := idToRingBufferMap.Load("TestCacheOverflow")
	if !ok {
		t.Error("Expected ring buffer to be created for key")
	}
	if rb.Size() != cacheSize {
		t.Errorf("Expected ring buffer size to be %d (cacheSize), got %d", cacheSize, rb.Size())
	}

	// Test 2: Verify all points were stored on disk despite cache overflow
	storedPoints := ReadLastDataPoints("TestCacheOverflow", len(points))
	if len(storedPoints) != len(points) {
		t.Errorf("Expected %d points on disk, got %d", len(points), len(storedPoints))
	}

	// Verify points are in correct order and have correct values
	for i, point := range points {
		if storedPoints[i].Value != point.Value {
			t.Errorf("Expected value %f at position %d, got %f", point.Value, i, storedPoints[i].Value)
		}
		if storedPoints[i].Timestamp != point.Timestamp {
			t.Errorf("Expected timestamp %d at position %d, got %d", point.Timestamp, i, storedPoints[i].Timestamp)
		}
	}

	// Test 3: Verify lastValue and lastTimestamp have latest values
	lastVal, ok := lastValue.Load("TestCacheOverflow")
	if !ok || lastVal != points[len(points)-1].Value {
		t.Errorf("Expected lastValue to be %f, got %v", points[len(points)-1].Value, lastVal)
	}
	lastTs, ok := lastTimestamp.Load("TestCacheOverflow")
	if !ok || lastTs != points[len(points)-1].Timestamp {
		t.Errorf("Expected lastTimestamp to be %d, got %v", points[len(points)-1].Timestamp, lastTs)
	}
}

func TestStoreDataPointBufferWithNonZeroCacheMultiplePoints(t *testing.T) {
	cleanup()
	defer cleanup()

	// Set non-zero cache size
	cacheSize = 5

	// Test data - multiple points
	now := time.Now().Unix()
	points := []models.DataPoint{
		{Key: "TestMultiCache", Timestamp: now, Value: 1.0},
		{Key: "TestMultiCache", Timestamp: now + 1, Value: 2.0},
		{Key: "TestMultiCache", Timestamp: now + 2, Value: 3.0},
	}

	// Store multiple data points
	for _, point := range points {
		StoreDataPointBuffer(point)
	}

	// Test 1: Verify all points are in ring buffer
	rb, ok := idToRingBufferMap.Load("TestMultiCache")
	if !ok {
		t.Error("Expected ring buffer to be created for key")
	}
	if rb.Size() != len(points) {
		t.Errorf("Expected %d points in ring buffer, got %d", len(points), rb.Size())
	}

	// Test 2: Verify all points were stored on disk
	storedPoints := ReadLastDataPoints("TestMultiCache", len(points))
	if len(storedPoints) != len(points) {
		t.Errorf("Expected %d points, got %d", len(points), len(storedPoints))
	}

	// Verify points are in correct order and have correct values
	for i, point := range points {
		if storedPoints[i].Value != point.Value {
			t.Errorf("Expected value %f at position %d, got %f", point.Value, i, storedPoints[i].Value)
		}
		if storedPoints[i].Timestamp != point.Timestamp {
			t.Errorf("Expected timestamp %d at position %d, got %d", point.Timestamp, i, storedPoints[i].Timestamp)
		}
	}

	// Test 3: Verify lastValue and lastTimestamp have latest values
	lastVal, ok := lastValue.Load("TestMultiCache")
	if !ok || lastVal != points[len(points)-1].Value {
		t.Errorf("Expected lastValue to be %f, got %v", points[len(points)-1].Value, lastVal)
	}
	lastTs, ok := lastTimestamp.Load("TestMultiCache")
	if !ok || lastTs != points[len(points)-1].Timestamp {
		t.Errorf("Expected lastTimestamp to be %d, got %v", points[len(points)-1].Timestamp, lastTs)
	}
}

func TestStoreDataPointBufferWithNonZeroCache(t *testing.T) {
	cleanup()
	defer cleanup()

	// Set non-zero cache size
	cacheSize = 10

	// Test data
	dataPoint := models.DataPoint{
		Key:       "TestNonZeroCache",
		Timestamp: time.Now().Unix(),
		Value:     42.5,
	}

	// Store the data point
	StoreDataPointBuffer(dataPoint)

	// Test 1: Verify data was stored in ring buffer
	rb, ok := idToRingBufferMap.Load(dataPoint.Key)
	if !ok {
		t.Error("Expected ring buffer to be created for key")
	}
	if rb.Size() != 1 {
		t.Errorf("Expected 1 point in ring buffer, got %d", rb.Size())
	}

	// Test 2: Verify data was stored on disk by reading it back
	points := ReadLastDataPoints("TestNonZeroCache", 1)
	if len(points) != 1 {
		t.Errorf("Expected 1 point, got %d", len(points))
	}
	if points[0].Value != dataPoint.Value {
		t.Errorf("Expected value %f, got %f", dataPoint.Value, points[0].Value)
	}

	// Test 3: Verify lastValue and lastTimestamp were updated
	lastVal, ok := lastValue.Load(dataPoint.Key)
	if !ok || lastVal != dataPoint.Value {
		t.Errorf("Expected lastValue to be %f, got %v", dataPoint.Value, lastVal)
	}
	lastTs, ok := lastTimestamp.Load(dataPoint.Key)
	if !ok || lastTs != dataPoint.Timestamp {
		t.Errorf("Expected lastTimestamp to be %d, got %v", dataPoint.Timestamp, lastTs)
	}
}

func TestStoreDataPointBufferWithZeroCache(t *testing.T) {
	cleanup()
	defer cleanup()

	// Set cache size to 0
	cacheSize = 0

	// Test data
	dataPoint := models.DataPoint{
		Key:       "TestZeroCache",
		Timestamp: time.Now().Unix(),
		Value:     42.5,
	}

	// Store the data point
	StoreDataPointBuffer(dataPoint)

	// Verify data was stored by reading it back
	points := ReadLastDataPoints("TestZeroCache", 1)
	if len(points) != 1 {
		t.Errorf("Expected 1 point, got %d", len(points))
	}
	if points[0].Value != dataPoint.Value {
		t.Errorf("Expected value %f, got %f", dataPoint.Value, points[0].Value)
	}
	if points[0].Timestamp != dataPoint.Timestamp {
		t.Errorf("Expected timestamp %d, got %d", dataPoint.Timestamp, points[0].Timestamp)
	}
}

func TestInitKey(t *testing.T) {
	cleanup()
	defer cleanup()

	// Test initializing a new key
	testID := "test_init_key"
	InitKey(testID)

	// Verify files were created
	if _, err := os.Stat(utils.DataDir + "/" + testID + ".aof"); os.IsNotExist(err) {
		t.Errorf("Expected .aof file to be created for %s", testID)
	}
	if _, err := os.Stat(utils.DataDir + "/" + testID + ".idx"); os.IsNotExist(err) {
		t.Errorf("Expected .idx file to be created for %s", testID)
	}

	// Verify ID was added to allIds
	ids := GetAllIds()
	found := false
	for _, id := range ids {
		if id == testID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected ID %s to be in allIds", testID)
	}

	InitKey("")
}

func TestRenameKey(t *testing.T) {
	cleanup()
	defer cleanup()

	// Create and initialize a test key
	oldID := "test_rename_old"
	newID := "test_rename_new"

	// Store some data to create the files
	dataPoint := models.DataPoint{
		Key:       oldID,
		Timestamp: time.Now().Unix(),
		Value:     42.5,
	}
	StoreDataPointBuffer(dataPoint)

	// Verify old key exists in memory state
	if !allIds.Contains(oldID) {
		t.Fatal("old key should exist before rename")
	}
	oldCount, _ := GetKeyCount(oldID)
	if oldCount != 1 {
		t.Fatalf("expected 1 data point for old key, got %d", oldCount)
	}

	// Rename the key
	RenameKey(oldID, newID)

	// Verify old files don't exist
	if _, err := os.Stat(utils.DataDir + "/" + oldID + ".aof"); !os.IsNotExist(err) {
		t.Errorf("Old .aof file still exists for %s", oldID)
	}
	if _, err := os.Stat(utils.DataDir + "/" + oldID + ".idx"); !os.IsNotExist(err) {
		t.Errorf("Old .idx file still exists for %s", oldID)
	}

	// Verify new files exist
	if _, err := os.Stat(utils.DataDir + "/" + newID + ".aof"); os.IsNotExist(err) {
		t.Errorf("Expected .aof file to exist for %s", newID)
	}
	if _, err := os.Stat(utils.DataDir + "/" + newID + ".idx"); os.IsNotExist(err) {
		t.Errorf("Expected .idx file to exist for %s", newID)
	}

	// Verify ID changes in allIds
	ids := GetAllIds()
	foundOld := false
	foundNew := false
	for _, id := range ids {
		if id == oldID {
			foundOld = true
		}
		if id == newID {
			foundNew = true
		}
	}
	if foundOld {
		t.Errorf("Old ID %s should not be in allIds", oldID)
	}
	if !foundNew {
		t.Errorf("New ID %s should be in allIds", newID)
	}

	// Verify in-memory state transferred
	if _, ok := idToCountMap.Load(oldID); ok {
		t.Error("old key count should be removed from idToCountMap")
	}
	newCount, ok := GetKeyCount(newID)
	if !ok || newCount != oldCount {
		t.Errorf("expected count %d for new key, got %d (ok=%v)", oldCount, newCount, ok)
	}

	// Verify data can be read from new key
	points := ReadLastDataPoints(newID, 1)
	if len(points) != 1 {
		t.Fatalf("expected 1 data point under new key, got %d", len(points))
	}
	if points[0].Value != 42.5 {
		t.Errorf("expected value 42.5, got %f", points[0].Value)
	}
	if points[0].Key != newID {
		t.Errorf("expected key %s, got %s", newID, points[0].Key)
	}

	// Verify old key has no data
	oldPoints := ReadLastDataPoints(oldID, 1)
	if len(oldPoints) != 0 {
		t.Errorf("old key should have no data, got %d points", len(oldPoints))
	}

	RenameKey("", "")
}

func TestRenameKeyPreservesData(t *testing.T) {
	cleanup()
	defer cleanup()

	oldID := "rename_data_test_old"
	newID := "rename_data_test_new"

	// Write multiple data points
	for i := 0; i < 10; i++ {
		StoreDataPointBuffer(models.DataPoint{
			Key:       oldID,
			Timestamp: time.Now().Unix() + int64(i),
			Value:     float64(i) * 10,
		})
	}

	// Verify last value/timestamp before rename
	lastBefore, _ := lastTimestamp.Load(oldID)
	valBefore, _ := lastValue.Load(oldID)

	RenameKey(oldID, newID)

	// Verify last value/timestamp migrated
	lastAfter, ok := lastTimestamp.Load(newID)
	if !ok || lastAfter != lastBefore {
		t.Errorf("lastTimestamp not migrated: old=%d new=%d ok=%v", lastBefore, lastAfter, ok)
	}
	valAfter, ok := lastValue.Load(newID)
	if !ok || valAfter != valBefore {
		t.Errorf("lastValue not migrated: old=%f new=%f ok=%v", valBefore, valAfter, ok)
	}

	// Read all data from new key
	points := ReadDataPoints(newID, 0, time.Now().Unix()+100, 0, "avg")
	if len(points) != 10 {
		t.Errorf("expected 10 data points under new key, got %d", len(points))
	}
}

func TestGetAllIdsWithCount(t *testing.T) {
	cleanup()
	defer cleanup()

	// Create test data with different sizes
	testData := []struct {
		id     string
		points int
	}{
		{"test1", 100},
		{"test2", 50},
		{"test3", 75},
	}

	// Store test data points
	for _, td := range testData {
		for i := 0; i < td.points; i++ {
			dataPoint := models.DataPoint{
				Key:       td.id,
				Timestamp: time.Now().Unix() + int64(i),
				Value:     float64(i),
			}
			StoreDataPointBuffer(dataPoint)
		}
	}

	// Get all IDs with count
	keyCounts := GetAllIdsWithCount()

	// Verify counts
	for _, td := range testData {
		found := false
		for _, kc := range keyCounts {
			if kc.Key == td.id {
				found = true
				// Note: The actual count might be different due to file size calculation
				// We just verify that the count is greater than 0
				if kc.Count <= 0 {
					t.Errorf("Expected count > 0 for key %s, got %d", td.id, kc.Count)
				}
				break
			}
		}
		if !found {
			t.Errorf("Key %s not found in results", td.id)
		}
	}
}

// TestGetAllIdsWithCountAfterEviction verifies that counts stay accurate even
// when a key's file handle has been evicted from the LRU cache. The quota
// reconciler and idswithcount rely on this.
func TestGetAllIdsWithCountAfterEviction(t *testing.T) {
	cleanup()
	defer cleanup()

	id := "test_evicted_count"
	const points = 42
	for i := 0; i < points; i++ {
		StoreDataPointBuffer(models.DataPoint{
			Key:       id,
			Timestamp: time.Now().Unix() + int64(i),
			Value:     float64(i),
		})
	}

	// Evict the handle from the LRU (simulates a key that hasn't been
	// touched in a while on a busy server).
	dataFileHandles.Delete(id + ".aof")

	keyCounts := GetAllIdsWithCount()
	for _, kc := range keyCounts {
		if kc.Key == id {
			if kc.Count != points {
				t.Errorf("Expected count %d after eviction, got %d", points, kc.Count)
			}
			return
		}
	}
	t.Errorf("Key %s not found in results", id)
}

func TestGetDataFileSize(t *testing.T) {
	cleanup()
	defer cleanup()

	testID := "TestGetDataFileSize"
	dataPoint := models.DataPoint{
		Key:       testID,
		Timestamp: time.Now().Unix(),
		Value:     42.5,
	}
	StoreDataPointBuffer(dataPoint)

	// Test getting size of an existing (open) file
	size, ok := GetDataFileSize(testID + ".aof")
	if !ok {
		t.Error("Expected size for existing key")
	}
	if size != 16 {
		t.Errorf("Expected size 16 for one record, got %d", size)
	}

	// Test getting size of a non-existent file (truly unique name)
	if _, ok := GetDataFileSize("__very_unlikely_nonexistent_file__.aof"); ok {
		t.Error("Expected false for non-existent file")
	}
}

func TestGetKeyCount(t *testing.T) {
	cleanup()
	defer cleanup()

	testID := "TestGetKeyCount"
	dataPoint := models.DataPoint{
		Key:       testID,
		Timestamp: time.Now().Unix(),
		Value:     1.0,
	}
	StoreDataPointBuffer(dataPoint)

	// Test getting count for existing key
	count, ok := GetKeyCount(testID)
	if !ok {
		t.Error("Expected key to exist")
	}
	if count < 1 {
		t.Errorf("Expected count >= 1, got %d", count)
	}

	// Test getting count for non-existent key (use truly unique name)
	_, ok = GetKeyCount("__nonexistent_key_with_very_unlikely_name__")
	if ok {
		t.Error("Expected false for non-existent key")
	}
}

func TestCompactKey(t *testing.T) {
	cleanup()
	defer cleanup()

	testID := "TestCompactKey"
	now := time.Now().Unix()

	// Write test data points
	for i := 0; i < 100; i++ {
		StoreDataPointBuffer(models.DataPoint{
			Key:       testID,
			Timestamp: now + int64(i),
			Value:     float64(i),
		})
	}

	// Verify data exists before compaction
	beforePoints := ReadLastDataPoints(testID, 100)
	if len(beforePoints) != 100 {
		t.Fatalf("Expected 100 points before compaction, got %d", len(beforePoints))
	}

	// Compact the key
	err := CompactKey(testID)
	if err != nil {
		t.Fatalf("CompactKey failed: %v", err)
	}

	// Verify data is still intact after compaction
	afterPoints := ReadLastDataPoints(testID, 100)
	if len(afterPoints) != 100 {
		t.Errorf("Expected 100 points after compaction, got %d", len(afterPoints))
	}

	// Verify values are correct
	for i := 0; i < 100; i++ {
		if afterPoints[i].Value != float64(i) {
			t.Errorf("At index %d: expected value %f, got %f", i, float64(i), afterPoints[i].Value)
		}
	}
}

func TestCompactKeyNonExistent(t *testing.T) {
	cleanup()
	defer cleanup()

	err := CompactKey("nonexistent_key")
	if err == nil {
		t.Error("Expected error for non-existent key")
	}
}

func TestCompactKeyEmptyKey(t *testing.T) {
	err := CompactKey("")
	if err == nil {
		t.Error("Expected error for empty key")
	}
}

func TestReloadKey(t *testing.T) {
	cleanup()
	defer cleanup()

	// Create a key with data
	key := "test_reload"
	StoreDataPointBuffer(models.DataPoint{
		Key:       key,
		Timestamp: time.Now().Unix(),
		Value:     99.9,
	})

	// Verify key exists
	if !allIds.Contains(key) {
		t.Fatal("key should exist before reload")
	}

	// Reload the key
	ok := ReloadKey(key)
	if !ok {
		t.Error("ReloadKey should return true for existing key")
	}

	// Verify key still exists and data is accessible
	if !allIds.Contains(key) {
		t.Error("key should still exist after reload")
	}

	points := ReadLastDataPoints(key, 1)
	if len(points) != 1 || points[0].Value != 99.9 {
		t.Errorf("data should be preserved after reload: got %d points, value=%f", len(points), points[0].Value)
	}
}

func TestReloadKeyEmptyKey(t *testing.T) {
	if ReloadKey("") {
		t.Error("ReloadKey should return false for empty key")
	}
}

func TestReloadKeyNonExistent(t *testing.T) {
	if ReloadKey("nonexistent_reload_test") {
		t.Error("ReloadKey should return false for non-existent key")
	}
}

func TestDeleteKeyEmpty(t *testing.T) {
	// Should not panic, just return
	DeleteKey("")
}

func TestDeleteDataPointsInvalidOperator(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_delete_invalid_op"
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: time.Now().Unix(), Value: 1.0})

	// hasValue=true but operator is invalid (not ">" or "<")
	removed := DeleteDataPoints(key, "=", 0.5, true, 0, 0)
	if removed != 0 {
		t.Errorf("Expected 0 removed for invalid operator, got %d", removed)
	}
}

func TestDeleteDataPointsEmptyKey(t *testing.T) {
	removed := DeleteDataPoints("", ">", 0.5, true, 0, 0)
	if removed != 0 {
		t.Errorf("Expected 0 for empty key, got %d", removed)
	}
}

func TestTryOverwriteNonExistentTimestamp(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_overwrite_miss"
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: 1000, Value: 1.0})

	// Try to overwrite a timestamp that doesn't exist
	overwritten := tryOverwriteSingleTimestampValue(key, models.DataPoint{Key: key, Timestamp: 2000, Value: 99.0})
	if overwritten {
		t.Error("Expected false for non-existent timestamp")
	}
}

func TestTryOverwriteNonExistentKey(t *testing.T) {
	// Try to overwrite on a key that doesn't exist
	overwritten := tryOverwriteSingleTimestampValue("nonexistent_overwrite", models.DataPoint{Timestamp: 1000, Value: 1.0})
	if overwritten {
		t.Error("Expected false for non-existent key")
	}
}

func TestReadLastDataPointsFromCache(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_read_last_cached"
	originalCacheSize := cacheSize
	cacheSize = 5
	defer func() { cacheSize = originalCacheSize }()

	// Store data through StoreDataPointBuffer which populates both ring buffer and lastValue
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: 1000, Value: 88.8})
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: 2000, Value: 99.9})

	points := ReadLastDataPoints(key, 1)
	if len(points) != 1 {
		t.Fatalf("Expected 1 point from cache, got %d", len(points))
	}
	if points[0].Value != 99.9 {
		t.Errorf("Expected latest cached value 99.9, got %f", points[0].Value)
	}
}

func TestReadFiledDataPointsWithoutIndex(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_read_no_index"
	// Write data but delete the index file to trigger the no-index path
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: 1000, Value: 42.0})
	FlushRemainingDataPoints()

	// Remove the index file
	os.Remove(utils.DataDir + "/" + key + ".idx")

	// Close and delete index handle from cache so it re-opens
	if ref, ok := refFromLRU(indexFileHandles, key+".idx"); ok {
		ref.release()
	}
	indexFileHandles.Delete(key + ".idx")

	points := ReadDataPoints(key, 0, 2000, 0, "avg")
	if len(points) != 1 || points[0].Value != 42.0 {
		t.Errorf("Expected 1 point with value 42.0, got %d points", len(points))
	}
}

func TestReadLastFiledEdgeCases(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_read_last_edge"
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: 1000, Value: 1.0})
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: 2000, Value: 2.0})

	// Read more than available
	points, err := readLastFiledDataPoints(key, 100)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(points) != 2 {
		t.Errorf("Expected 2 points, got %d", len(points))
	}
}

func TestGetTotalDataPoints(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_total_count"
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: 1000, Value: 1.0})
	StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: 2000, Value: 2.0})

	total := GetTotalDataPoints()
	if total < 2 {
		t.Errorf("Expected at least 2 total data points, got %d", total)
	}
}
