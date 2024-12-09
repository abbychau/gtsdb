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
	files, _ := os.ReadDir(utils.DataDir)
	for _, file := range files {
		os.Remove(utils.DataDir + "/" + file.Name())
	}
}

func TestStoreAndReadDataPoints(t *testing.T) {
	cleanup()
	defer cleanup()

	// Test data
	dataPoint := models.DataPoint{
		ID:        "TestStoreAndReadDataPoints",
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
		{ID: "test1", Timestamp: 1000, Value: 42.5},
		{ID: "test1", Timestamp: 2000, Value: 43.5},
	}

	formatted := FormatDataPoints(points)
	expected := "test1,1000,42.50|test1,2000,43.50\n"
	if formatted != expected {
		t.Errorf("Expected %s, got %s", expected, formatted)
	}
}

func TestJsonFormatDataPoints(t *testing.T) {
	points := []models.DataPoint{
		{ID: "test1", Timestamp: 1000, Value: 42.5},
	}

	formatted := JsonFormatDataPoints(points)
	expected := `[{"id":"test1","timestamp":1000,"value":42.5}]`
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
		{ID: "test2", Timestamp: now, Value: 1.0},
		{ID: "test2", Timestamp: now + 1, Value: 2.0},
		{ID: "test2", Timestamp: now + 2, Value: 3.0},
		{ID: "test2", Timestamp: now + 3, Value: 4.0},
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
		ID:        "test3",
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
			ID:        "TestReadLast",
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
		ID:        "test_empty",
		Timestamp: 1000,
		Value:     42.5,
	}
	StoreDataPointBuffer(dataPoint)

	result = ReadDataPoints("test_empty", 2000, 3000, 1, "avg")
	if len(result) != 0 {
		t.Errorf("Expected empty result for invalid time range, got %d points", len(result))
	}
}
