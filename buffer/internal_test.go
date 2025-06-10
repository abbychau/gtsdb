package buffer

import (
	"gtsdb/models"
	"gtsdb/synchronous"
	"gtsdb/utils"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	utils.DataDir = "data"
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
			Key:       id,
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
			Key:       id,
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
		{Key: id, Timestamp: 1111111000, Value: 1.0},
		{Key: id, Timestamp: 1111112000, Value: 2.0},
		{Key: id, Timestamp: 3000, Value: 3.0},
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
		{Key: id, Timestamp: now - 4, Value: 1.0},
		{Key: id, Timestamp: now - 3, Value: 2.0},
		{Key: id, Timestamp: now - 2, Value: 3.0},
		{Key: id, Timestamp: now - 1, Value: 4.0},
		{Key: id, Timestamp: now, Value: 5.0},
	}

	// Store in buffer
	rb := synchronous.NewRingBuffer[models.DataPoint](cacheSize)
	for _, dp := range dataPoints {
		rb.Push(dp)
	}
	idToRingBufferMap.Store(id, rb)

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
	idToRingBufferMap.Clear()
}

func TestReadLastBufferedDataPoints(t *testing.T) {
	id := "TestReadLastBufferedDataPoints"

	t.Run("last timestamp exists", func(t *testing.T) {
		testId := "test-last-value"
		expectedTimestamp := int64(1234567)
		expectedValue := 42.0

		lastTimestamp.Store(testId, expectedTimestamp)
		lastValue.Store(testId, expectedValue)

		points := readLastBufferedDataPoints(testId, 1)

		if len(points) != 1 {
			t.Errorf("Expected 1 point, got %d points", len(points))
		}
		if len(points) > 0 {
			if points[0].Timestamp != expectedTimestamp {
				t.Errorf("Expected timestamp %d, got %d", expectedTimestamp, points[0].Timestamp)
			}
			if points[0].Value != expectedValue {
				t.Errorf("Expected value %f, got %f", expectedValue, points[0].Value)
			}
			if points[0].Key != testId {
				t.Errorf("Expected key %s, got %s", testId, points[0].Key)
			}
		}

		lastTimestamp.Delete(testId)
		lastValue.Delete(testId)
	})

	t.Run("map load not ok", func(t *testing.T) {
		points := readLastBufferedDataPoints("nonexistent", 1)
		if len(points) != 0 {
			t.Errorf("Expected empty slice, got %d points", len(points))
		}
	})

	// Setup test data
	cacheSize = 5
	rb := synchronous.NewRingBuffer[models.DataPoint](cacheSize)
	for i := 0; i < 3; i++ {
		rb.Push(models.DataPoint{Timestamp: int64(i), Value: float64(i)})
	}
	idToRingBufferMap.Store(id, rb)

	t.Run("count greater than size", func(t *testing.T) {
		points := readLastBufferedDataPoints(id, 10)
		if len(points) != 3 {
			t.Errorf("Expected 3 points, got %d", len(points))
		}
	})

	t.Run("count is zero", func(t *testing.T) {
		points := readLastBufferedDataPoints(id, 0)
		if len(points) != 0 {
			t.Errorf("Expected 0 points, got %d", len(points))
		}
	})

	// Cleanup
	idToRingBufferMap.Clear()
}

func TestDownsampleDataPointsEdgeCases(t *testing.T) {
	t.Run("empty data points", func(t *testing.T) {
		result := downsampleDataPoints([]models.DataPoint{}, 1000, "avg")
		if len(result) != 0 {
			t.Errorf("Expected empty result, got %d points", len(result))
		}
	})

	t.Run("invalid aggregation", func(t *testing.T) {
		dataPoints := []models.DataPoint{
			{Timestamp: 1000, Value: 1.0},
			{Timestamp: 2000, Value: 2.0},
		}
		result := downsampleDataPoints(dataPoints, 1000, "invalid")
		if len(result) == 0 {
			t.Error("Expected non-empty result with default avg aggregation")
		}
	})

	t.Run("min value update", func(t *testing.T) {
		dataPoints := []models.DataPoint{
			{Timestamp: 1000, Value: 3.0},
			{Timestamp: 1500, Value: 1.0},
			{Timestamp: 2000, Value: 2.0},
		}
		result := downsampleDataPoints(dataPoints, 2000, "min") // Use 2000 to group all points in one interval
		if len(result) != 1 || result[0].Value != 1 {
			t.Errorf("Expected min value 1, got %f, len: %d", result[0].Value, len(result))
		}
	})
}

func TestReadBufferedDataPointsEdgeCases(t *testing.T) {
	id := "TestReadBufferedDataPointsEdgeCases"

	t.Run("cache size zero", func(t *testing.T) {
		originalSize := cacheSize
		cacheSize = 0
		points := readBufferedDataPoints(id, 0, 1000)
		if len(points) != 0 {
			t.Errorf("Expected empty result when cache size is 0")
		}
		cacheSize = originalSize
	})

	t.Run("map load not ok", func(t *testing.T) {
		points := readBufferedDataPoints("nonexistent", 0, 1000)
		if len(points) != 0 {
			t.Errorf("Expected empty result when id doesn't exist")
		}
	})
}

func TestPrepareFileHandlesPanic(t *testing.T) {
	originalDataDir := utils.DataDir
	utils.DataDir = "/nonexistent/directory"

	defer func() {
		utils.DataDir = originalDataDir
		if r := recover(); r == nil {
			t.Errorf("Expected panic for invalid directory")
		}
	}()

	prepareFileHandles("test.aof", dataFileHandles)
}

func TestPatchDataPointsEmptyKey(t *testing.T) {
	id := "TestPatchDataPointsEmptyKey"
	cleanTestFiles(id)

	// Initial data
	initialPoints := []models.DataPoint{
		{Key: id, Timestamp: 1000, Value: 1.0},
		{Key: id, Timestamp: 2000, Value: 2.0},
		{Key: id, Timestamp: 4000, Value: 4.0},
	}
	storeDataPoints(id, initialPoints)

	// Patch data
	patchPoints := []models.DataPoint{
		{Key: id, Timestamp: 3000, Value: 3.0},
		{Key: id, Timestamp: 5000, Value: 5.0},
	}
	PatchDataPoints(patchPoints, id)

	// Verify
	result := readFiledDataPoints(id, 0, 6000)
	expected := 5
	if len(result) != expected {
		t.Errorf("Expected %d points, got %d", expected, len(result))
	}

	// Verify order and values
	expectedPoints := []struct {
		ts    int64
		value float64
	}{
		{1000, 1.0},
		{2000, 2.0},
		{3000, 3.0},
		{4000, 4.0},
		{5000, 5.0},
	}

	for i, exp := range expectedPoints {
		if result[i].Timestamp != exp.ts || result[i].Value != exp.value {
			t.Errorf("Point %d: expected {ts: %d, val: %f}, got {ts: %d, val: %f}",
				i, exp.ts, exp.value, result[i].Timestamp, result[i].Value)
		}
	}

	cleanTestFiles(id)
}

func TestPatchDataPointsExistingTimestamps(t *testing.T) {
	id := "TestPatchDataPointsExistingTimestamps"
	cleanTestFiles(id)

	// Initial data
	initialPoints := []models.DataPoint{
		{Key: id, Timestamp: 1000, Value: 1.0},
		{Key: id, Timestamp: 2000, Value: 2.0},
		{Key: id, Timestamp: 3000, Value: 3.0},
	}
	storeDataPoints(id, initialPoints)

	// Patch same timestamps with different values
	patchPoints := []models.DataPoint{
		{Key: id, Timestamp: 1000, Value: 10.0},
		{Key: id, Timestamp: 2000, Value: 20.0},
	}
	PatchDataPoints(patchPoints, id)

	// Verify
	result := readFiledDataPoints(id, 0, 4000)
	if len(result) != 3 {
		t.Errorf("Expected 3 points, got %d", len(result))
	}

	// Check updated values
	expectedPoints := []struct {
		ts    int64
		value float64
	}{
		{1000, 10.0}, // Original values should be preserved
		{2000, 20.0},
		{3000, 3.0},
	}

	for i, exp := range expectedPoints {
		if result[i].Timestamp != exp.ts || result[i].Value != exp.value {
			t.Errorf("Point %d: expected {ts: %d, val: %f}, got {ts: %d, val: %f}",
				i, exp.ts, exp.value, result[i].Timestamp, result[i].Value)
		}
	}

	cleanTestFiles(id)
}

func TestPatchDataPointsEmptyDataset(t *testing.T) {
	id := "TestPatchDataPointsEmptyDataset"
	cleanTestFiles(id)

	// Patch data into empty dataset
	patchPoints := []models.DataPoint{
		{Key: id, Timestamp: 1000, Value: 1.0},
		{Key: id, Timestamp: 2000, Value: 2.0},
	}
	PatchDataPoints(patchPoints, id)

	// Verify
	result := readFiledDataPoints(id, 0, 3000)
	if len(result) != 2 {
		t.Errorf("Expected 2 points, got %d", len(result))
	}

	expectedPoints := []struct {
		ts    int64
		value float64
	}{
		{1000, 1.0},
		{2000, 2.0},
	}

	for i, exp := range expectedPoints {
		if result[i].Timestamp != exp.ts || result[i].Value != exp.value {
			t.Errorf("Point %d: expected {ts: %d, val: %f}, got {ts: %d, val: %f}",
				i, exp.ts, exp.value, result[i].Timestamp, result[i].Value)
		}
	}

	cleanTestFiles(id)
}

func TestPatchDataPointsConcurrent(t *testing.T) {

	id := "TestPatchDataPointsConcurrent"
	cleanTestFiles(id)

	// Initial data
	initialPoints := []models.DataPoint{
		{Key: id, Timestamp: 1000, Value: 1.0},
		{Key: id, Timestamp: 2000, Value: 2.0},
	}
	storeDataPoints(id, initialPoints)

	// Run concurrent patches
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			patchPoints := []models.DataPoint{
				{Key: id, Timestamp: int64(3000 + i*1000), Value: float64(3 + i)},
			}
			PatchDataPoints(patchPoints, id)
		}(i)
	}
	wg.Wait()

	// Verify final state
	result := readFiledDataPoints(id, 0, 6000)
	if len(result) != 5 {
		t.Errorf("Expected 5 points, got %d", len(result))
	}

	// Verify all timestamps are unique and sorted
	for i := 1; i < len(result); i++ {
		if result[i].Timestamp <= result[i-1].Timestamp {
			t.Errorf("Points not properly ordered at index %d", i)
		}
	}
}
