package main

import (
	models "gtsdb/models"
	"os"
	"path/filepath"
	"testing"
)

func setupTestFiles(t *testing.T) (string, func()) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "gtsdb_test")
	if err != nil {
		t.Fatal(err)
	}

	// Return cleanup function
	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return tmpDir, cleanup
}

func TestMetaCount(t *testing.T) {
	tmpDir, cleanup := setupTestFiles(t)
	defer cleanup()

	metaPath := filepath.Join(tmpDir, "meta.txt")
	metaFile, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer metaFile.Close()

	// Test writing count
	writeMetaCount(metaFile, 42)

	// Test reading count
	count := readMetaCount(metaFile)
	if count != 42 {
		t.Errorf("Expected count 42, got %d", count)
	}
}

func TestDownsampleDataPoints(t *testing.T) {
	testCases := []struct {
		name        string
		points      []models.DataPoint
		downsample  int
		aggregation string
		expected    int
	}{
		{
			name: "avg_downsample",
			points: []models.DataPoint{
				{ID: "test1", Timestamp: 1, Value: 1.0},
				{ID: "test1", Timestamp: 2, Value: 2.0},
				{ID: "test1", Timestamp: 3, Value: 3.0},
				{ID: "test1", Timestamp: 4, Value: 4.0},
			},
			downsample:  2,
			aggregation: "avg",
			expected:    2, // Should combine into 2 points
		},
		{
			name: "max_downsample",
			points: []models.DataPoint{
				{ID: "test1", Timestamp: 1, Value: 1.0},
				{ID: "test1", Timestamp: 2, Value: 2.0},
			},
			downsample:  2,
			aggregation: "max",
			expected:    1, // Should combine into 1 point
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := downsampleDataPoints(tc.points, tc.downsample, tc.aggregation)
			if len(result) != tc.expected {
				t.Errorf("Expected %d points, got %d", tc.expected, len(result))
			}
		})
	}
}

func TestFormatDataPoints(t *testing.T) {
	points := []models.DataPoint{
		{ID: "test1", Timestamp: 1, Value: 1.5},
		{ID: "test1", Timestamp: 2, Value: 2.5},
	}

	expected := "test1,1,1.50|test1,2,2.50\n"
	result := formatDataPoints(points)

	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestReadDataPoints(t *testing.T) {
	tmpDir, cleanup := setupTestFiles(t)
	defer cleanup()

	// Set global data directory
	dataDir = tmpDir

	// Create test data file
	id := "test1"
	dataFile := dataFileById(id)
	defer dataFile.Close()

	// Write test data
	testData := "1000,1.5\n2000,2.5\n3000,3.5\n"
	dataFile.WriteString(testData)

	// Test reading with time range
	points := readDataPoints(id, 1500, 2500, 1, "avg")
	if len(points) != 1 {
		t.Errorf("Expected 1 point, got %d", len(points))
	}

	// Test with downsampling
	points = readDataPoints(id, 1000, 3000, 2000, "avg")
	if len(points) != 2 {
		t.Errorf("Expected 2 points, got %d", len(points))
	}
}
