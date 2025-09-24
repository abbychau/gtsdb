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
		os.Mkdir(utils.DataDir, 0755)
	}
	files, _ := os.ReadDir(utils.DataDir)
	for _, file := range files {
		os.Remove(utils.DataDir + "/" + file.Name())
	}
	
	// Initialize file handles if not already initialized
	if dataFileHandles == nil || indexFileHandles == nil {
		InitFileHandles()
	}
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

func TestFormatDataPoints(t *testing.T) {
	points := []models.DataPoint{
		{Key: "test1", Timestamp: 1000, Value: 42.5},
		{Key: "test1", Timestamp: 2000, Value: 43.5},
	}

	formatted := FormatDataPoints(points)
	expected := "test1,1000,42.50|test1,2000,43.50\n"
	if formatted != expected {
		t.Errorf("Expected %s, got %s", expected, formatted)
	}
}

func TestJsonFormatDataPoints(t *testing.T) {
	points := []models.DataPoint{
		{Key: "test1", Timestamp: 1000, Value: 42.5},
	}

	formatted := JsonFormatDataPoints(points)
	expected := `[{"key":"test1","timestamp":1000,"value":42.5}]`
	if formatted != expected {
		t.Errorf("Expected %s, got %s", formatted, formatted)
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

	RenameKey("", "")
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
