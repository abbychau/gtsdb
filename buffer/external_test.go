package buffer

import (
	"gtsdb/models"
	"gtsdb/utils"
	"os"
	"testing"
	"time"
)

func init() {
	utils.DataDir = "testdata"
	os.MkdirAll(utils.DataDir, 0755)
}

func cleanup() {
	os.RemoveAll(utils.DataDir)
}

func TestStoreAndReadDataPoints(t *testing.T) {
	defer cleanup()

	// Test data
	dataPoint := models.DataPoint{
		ID:        "test1",
		Timestamp: time.Now().Unix(),
		Value:     42.5,
	}

	// Test storing
	StoreDataPointBuffer(dataPoint)

	// Test reading last point
	points := ReadLastDataPoints("test1", 1)
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
		t.Errorf("Expected %s, got %s", expected, formatted)
	}
}

func TestReadDataPointsWithDownsampling(t *testing.T) {
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
