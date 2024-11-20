package buffer

import (
	"gtsdb/models"
	"gtsdb/synchronous"
	"gtsdb/utils"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// Setup
	utils.DataDir = "testdata"
	err := os.MkdirAll(utils.DataDir, 0755)
	if err != nil {
		panic(err)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	os.RemoveAll(utils.DataDir)
	os.Exit(code)
}

func cleanTestFiles(id string) {
	utils.DataDir = "testdata"
	os.Remove(utils.DataDir + "/" + id + ".aof")
	os.Remove(utils.DataDir + "/" + id + ".meta")
	os.Remove(utils.DataDir + "/" + id + ".idx")
}

func TestStoreDataPoints(t *testing.T) {
	id := "TestStoreDataPoints"
	cleanTestFiles(id)

	dataPoints := []models.DataPoint{
		{ID: id, Timestamp: 1000, Value: 1.0},
		{ID: id, Timestamp: 2000, Value: 2.0},
		{ID: id, Timestamp: 3000, Value: 3.0},
	}

	storeDataPoints(id, dataPoints)

	// Verify stored data
	readPoints := readFiledDataPoints(id, 0, 4000)
	if len(readPoints) != 3 {
		t.Errorf("Expected 3 points, got %d", len(readPoints))
	}

	cleanTestFiles(id)
}

func TestReadFiledDataPoints(t *testing.T) {
	id := "TestReadFiledDataPoints"
	cleanTestFiles(id)

	// Store test data
	dataPoints := []models.DataPoint{
		{ID: id, Timestamp: 1000, Value: 1.0},
		{ID: id, Timestamp: 2000, Value: 2.0},
		{ID: id, Timestamp: 3000, Value: 3.0},
		{ID: id, Timestamp: 4000, Value: 4.0},
	}
	storeDataPoints(id, dataPoints)

	tests := []struct {
		name      string
		startTime int64
		endTime   int64
		want      int
	}{
		{"full range", 0, 5000, 4},
		{"partial range", 2000, 3000, 2},
		{"no data range", 5000, 6000, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points := readFiledDataPoints(id, tt.startTime, tt.endTime)
			if len(points) != tt.want {
				t.Errorf("readFiledDataPoints() got %d points, want %d", len(points), tt.want)
			}
		})
	}

	cleanTestFiles(id)
}

func TestReadLastFiledDataPoints(t *testing.T) {
	id := "TestReadLastFiledDataPoints"
	cleanTestFiles(id)
	utils.DataDir = "testdata"

	// Store test data
	dataPoints := []models.DataPoint{
		{ID: id, Timestamp: 1111111000, Value: 1.0},
		{ID: id, Timestamp: 1111112000, Value: 2.0},
		{ID: id, Timestamp: 3000, Value: 3.0},
	}
	storeDataPoints(id, dataPoints)

	tests := []struct {
		name  string
		count int
		want  int
	}{
		{"get all", 3, 3},
		{"get last two", 2, 2},
		{"get more than exists", 5, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points, err := readLastFiledDataPoints(id, tt.count)
			if err != nil {
				t.Errorf("readLastFiledDataPoints() error = %v", err)
			}
			if len(points) != tt.want {
				t.Errorf("readLastFiledDataPoints() got %d points, want %d", len(points), tt.want)
			}
		})
	}

	cleanTestFiles(id)
}

func TestDownsampleDataPoints(t *testing.T) {
	tests := []struct {
		name        string
		input       []models.DataPoint
		downsample  int
		aggregation string
		want        int
	}{
		{
			name: "average downsampling",
			input: []models.DataPoint{
				{Timestamp: 1000, Value: 1.0},
				{Timestamp: 2000, Value: 2.0},
				{Timestamp: 3000, Value: 3.0},
				{Timestamp: 4000, Value: 4.0},
			},
			downsample:  2000,
			aggregation: "avg",
			want:        2,
		},
		{
			name: "max downsampling",
			input: []models.DataPoint{
				{Timestamp: 1000, Value: 1.0},
				{Timestamp: 2000, Value: 2.0},
				{Timestamp: 3000, Value: 3.0},
				{Timestamp: 4000, Value: 4.0},
			},
			downsample:  2000,
			aggregation: "max",
			want:        2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := downsampleDataPoints(tt.input, tt.downsample, tt.aggregation)
			if len(result) != tt.want {
				t.Errorf("downsampleDataPoints() got %d points, want %d", len(result), tt.want)
			}
		})
	}
}

func TestBufferedDataPoints(t *testing.T) {
	id := "test4"
	cacheSize = 5

	// Test data
	now := time.Now().Unix()
	dataPoints := []models.DataPoint{
		{ID: id, Timestamp: now - 4, Value: 1.0},
		{ID: id, Timestamp: now - 3, Value: 2.0},
		{ID: id, Timestamp: now - 2, Value: 3.0},
		{ID: id, Timestamp: now - 1, Value: 4.0},
		{ID: id, Timestamp: now, Value: 5.0},
	}

	// Store in buffer
	rb := synchronous.NewRingBuffer[models.DataPoint](cacheSize)
	for _, dp := range dataPoints {
		rb.Push(dp)
	}
	idToRingBufferMap.Set(id, rb)

	t.Run("read buffered range", func(t *testing.T) {
		points := readBufferedDataPoints(id, now-3, now-1)
		if len(points) != 3 {
			t.Errorf("Expected 3 points, got %d", len(points))
		}
	})

	t.Run("read last buffered", func(t *testing.T) {
		points := readLastBufferedDataPoints(id, 2)
		if len(points) != 2 {
			t.Errorf("Expected 2 points, got %d", len(points))
		}
		if points[1].Value != 5.0 {
			t.Errorf("Expected last value 5.0, got %f", points[1].Value)
		}
	})

	// Cleanup
	idToRingBufferMap = nil
}
