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
	utils.DataDir = "../data"
	// Setup
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
	os.Remove(utils.DataDir + "/" + id + ".aof")
	os.Remove(utils.DataDir + "/" + id + ".idx")
}

func TestStoreDataPoints(t *testing.T) {
	id := "TestStoreDataPoints"
	cleanTestFiles(id)

	// Generate 16000 test data points
	dataPoints := make([]models.DataPoint, 16000)
	for i := 0; i < 16000; i++ {
		dataPoints[i] = models.DataPoint{
			ID:        id,
			Timestamp: int64(i * 1000), // Timestamps at 1-second intervals
			Value:     float64(i),
		}
	}

	storeDataPoints(id, dataPoints)

	// Verify stored data
	readPoints := readFiledDataPoints(id, 0, 16000000)
	if len(readPoints) != 16000 {
		t.Errorf("Expected 16000 points, got %d", len(readPoints))
	}

	// Verify some random points
	checkPoints := []struct {
		index int
		ts    int64
		value float64
	}{
		{0, 0, 0},
		{1000, 1000000, 1000},
		{15999, 15999000, 15999},
	}

	for _, cp := range checkPoints {
		point := readPoints[cp.index]
		if point.Timestamp != cp.ts || point.Value != cp.value {
			t.Errorf("Point at index %d: got {ts: %d, val: %f}, want {ts: %d, val: %f}",
				cp.index, point.Timestamp, point.Value, cp.ts, cp.value)
		}
	}

	cleanTestFiles(id)
}

func TestReadFiledDataPoints(t *testing.T) {
	id := "TestReadFiledDataPoints"
	cleanTestFiles(id)

	// Generate 16000 test data points
	dataPoints := make([]models.DataPoint, 16000)
	for i := 0; i < 16000; i++ {
		dataPoints[i] = models.DataPoint{
			ID:        id,
			Timestamp: int64(i * 1000), // Timestamps at 1-second intervals
			Value:     float64(i),
		}
	}
	storeDataPoints(id, dataPoints)

	tests := []struct {
		name      string
		startTime int64
		endTime   int64
		want      int
	}{
		{"full range", 0, 16000000, 16000},
		{"partial range", 5000000, 6000000, 1001}, // Should include points from 5000 to 6000
		{"no data range", 17000000, 18000000, 0},  // Outside range
		{"early range", 0, 1000000, 1001},         // First 1001 points
		{"middle range", 8000000, 8100000, 101},   // 101 points in the middle
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
