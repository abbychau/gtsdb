package handlers

import (
	"gtsdb/buffer"
	"gtsdb/models"
	"gtsdb/utils"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	utils.DataDir = "../data"
}

func TestHandleOperation(t *testing.T) {
	// Clear buffer before tests
	buffer.FlushRemainingDataPoints()

	t.Run("Write Operation", func(t *testing.T) {
		op := Operation{
			Operation: "write",
			Key:       "test1",
			Write: &WriteRequest{
				Value: 42.5,
			},
		}

		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Write operation failed: %s", resp.Message)
		}
	})

	t.Run("Write Operation with Custom Timestamp", func(t *testing.T) {
		timestamp := time.Now().Unix()
		op := Operation{
			Operation: "write",
			Key:       "test2",
			Write: &WriteRequest{
				Value:     23.1,
				Timestamp: timestamp,
			},
		}

		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Write operation with timestamp failed: %s", resp.Message)
		}
	})

	t.Run("Invalid Write Operation", func(t *testing.T) {
		op := Operation{
			Operation: "write",
		}

		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Invalid write operation should fail")
		}
	})

	t.Run("Read Operation LastX", func(t *testing.T) {
		// Write test data
		writeTestData(t, "test3", []float64{1.0, 2.0, 3.0})

		op := Operation{
			Operation: "read",

			Key: "test3",
			Read: &ReadRequest{
				LastX: 2,
			},
		}

		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Read operation failed: %s", resp.Message)
		}

		data, ok := resp.Data.([]models.DataPoint)
		if !ok {
			t.Fatal("Invalid response data type")
		}
		if len(data) != 2 {
			t.Errorf("Expected 2 data points, got %d", len(data))
		}
	})

	t.Run("Read Operation Time Range", func(t *testing.T) {
		now := time.Now().Unix()
		op := Operation{
			Operation: "read",

			Key: "test3",
			Read: &ReadRequest{
				StartTime: now - 3600,
				EndTime:   now,
			},
		}

		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Read operation failed: %s", resp.Message)
		}
	})

	t.Run("Read Operation Timestamp Range Without LastX", func(t *testing.T) {
		// Write test data with specific timestamps
		testKey := "test_timestamp_range"
		baseTime := time.Now().Unix() - 300 // 5 minutes ago
		
		// Write 5 data points over 4 minutes
		timestamps := []int64{
			baseTime,      // 5 minutes ago
			baseTime + 60, // 4 minutes ago
			baseTime + 120, // 3 minutes ago  
			baseTime + 180, // 2 minutes ago
			baseTime + 240, // 1 minute ago
		}
		values := []float64{10.0, 20.0, 30.0, 40.0, 50.0}

		for i, val := range values {
			op := Operation{
				Operation: "write",
				Key:       testKey,
				Write: &WriteRequest{
					Value:     val,
					Timestamp: timestamps[i],
				},
			}
			resp := HandleOperation(op)
			if !resp.Success {
				t.Fatalf("Failed to write test data: %s", resp.Message)
			}
		}

		// Test reading with timestamp range (without lastx)
		op := Operation{
			Operation: "read",
			Key:       testKey,
			Read: &ReadRequest{
				StartTime:   baseTime + 60,  // 4 minutes ago
				EndTime:     baseTime + 180, // 2 minutes ago
				Downsample: 1,
				Aggregation: "avg",
			},
		}

		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Timestamp-based read operation failed: %s", resp.Message)
		}

		data, ok := resp.Data.([]models.DataPoint)
		if !ok {
			t.Fatal("Invalid response data type")
		}

		// Should return 3 data points (indices 1, 2, 3) with values 20.0, 30.0, 40.0
		expectedCount := 3
		if len(data) != expectedCount {
			t.Errorf("Expected %d data points, got %d", expectedCount, len(data))
		}

		// Verify the values are correct
		expectedValues := []float64{20.0, 30.0, 40.0}
		for i, point := range data {
			if i < len(expectedValues) && point.Value != expectedValues[i] {
				t.Errorf("Expected value %f at index %d, got %f", expectedValues[i], i, point.Value)
			}
		}

		// Verify timestamps are within range
		for _, point := range data {
			if point.Timestamp < baseTime+60 || point.Timestamp > baseTime+180 {
				t.Errorf("Data point timestamp %d is outside expected range [%d, %d]", 
					point.Timestamp, baseTime+60, baseTime+180)
			}
		}

		// Verify ReadQueryParams shows timestamp-based query was used
		if resp.ReadQueryParams.StartTime != baseTime+60 {
			t.Errorf("Expected ReadQueryParams StartTime %d, got %d", baseTime+60, resp.ReadQueryParams.StartTime)
		}
		if resp.ReadQueryParams.EndTime != baseTime+180 {
			t.Errorf("Expected ReadQueryParams EndTime %d, got %d", baseTime+180, resp.ReadQueryParams.EndTime)
		}
		if resp.ReadQueryParams.LastX != 0 {
			t.Errorf("Expected ReadQueryParams LastX to be 0 (not used), got %d", resp.ReadQueryParams.LastX)
		}
	})

	t.Run("Multi-Read Operation Timestamp Range Without LastX", func(t *testing.T) {
		// Write test data for multiple keys
		testKeys := []string{"multi_test_1", "multi_test_2"}
		baseTime := time.Now().Unix() - 200
		
		for keyIndex, testKey := range testKeys {
			for i := 0; i < 3; i++ {
				op := Operation{
					Operation: "write",
					Key:       testKey,
					Write: &WriteRequest{
						Value:     float64((keyIndex+1)*10 + i), // multi_test_1: 10,11,12; multi_test_2: 20,21,22
						Timestamp: baseTime + int64(i*60),      // timestamps 60 seconds apart
					},
				}
				resp := HandleOperation(op)
				if !resp.Success {
					t.Fatalf("Failed to write test data: %s", resp.Message)
				}
			}
		}

		// Test multi-read with timestamp range
		op := Operation{
			Operation: "multi-read",
			Keys:      testKeys,
			Read: &ReadRequest{
				StartTime:    baseTime,
				EndTime:      baseTime + 120,
				Downsample: 1,
				Aggregation:  "avg",
			},
		}

		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Multi-read timestamp-based operation failed: %s", resp.Message)
		}

		if resp.MultiData == nil {
			t.Fatal("MultiData should not be nil")
		}

		// Verify both keys have data
		for _, key := range testKeys {
			data, exists := resp.MultiData[key]
			if !exists {
				t.Errorf("Expected data for key %s", key)
				continue
			}
			if len(data) != 3 {
				t.Errorf("Expected 3 data points for key %s, got %d", key, len(data))
			}
		}
	})

	t.Run("Invalid Read Operation", func(t *testing.T) {
		op := Operation{
			Operation: "read",
		}

		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Invalid read operation should fail")
		}
	})

	t.Run("Flush Operation", func(t *testing.T) {
		op := Operation{
			Operation: "flush",
		}

		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Flush operation failed: %s", resp.Message)
		}
	})

	t.Run("Invalid Operation", func(t *testing.T) {
		op := Operation{
			Operation: "invalid",
		}

		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Invalid operation should fail")
		}
	})
}

// Helper function to write test data
func writeTestData(t *testing.T, id string, values []float64) {
	for _, val := range values {
		op := Operation{
			Operation: "write",

			Key: id,
			Write: &WriteRequest{
				Value: val,
			},
		}
		resp := HandleOperation(op)
		if !resp.Success {
			t.Fatalf("Failed to write test data: %s", resp.Message)
		}
		time.Sleep(time.Millisecond) // Ensure different timestamps
	}
}
