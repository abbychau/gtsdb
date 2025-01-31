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
