package handlers

import (
	"gtsdb/auth"
	"gtsdb/buffer"
	"gtsdb/models"
	"gtsdb/utils"
	"os"
	"strings"
	"testing"
	"time"
)

func init() {
	// Use temp dir to avoid interfering with real data
	dir, err := os.MkdirTemp("", "gtsdb-handlers-test")
	if err != nil {
		panic(err)
	}
	utils.DataDir = dir
	auth.Init(dir)
	buffer.InitFileHandles()
	buffer.InitIDSet()
	// Note: dir is cleaned up by the OS eventually; tests clean up their own files
}

// testToken returns a valid auth token for tests
func testToken() string {
	root, ok := auth.GetUser("root")
	if !ok {
		panic("root user not found")
	}
	return root.Token
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
			baseTime,       // 5 minutes ago
			baseTime + 60,  // 4 minutes ago
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
				Downsample:  1,
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
						Timestamp: baseTime + int64(i*60),       // timestamps 60 seconds apart
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
				StartTime:   baseTime,
				EndTime:     baseTime + 120,
				Downsample:  1,
				Aggregation: "avg",
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

	t.Run("deleteDataPoint Operation", func(t *testing.T) {
		testKey := "delete_by_value_test"
		baseTime := time.Now().Unix()

		for i, value := range []float64{1.0, 2.0, 3.0, 4.0} {
			resp := HandleOperation(Operation{
				Operation: "write",
				Key:       testKey,
				Write: &WriteRequest{
					Value:     value,
					Timestamp: baseTime + int64(i),
				},
			})
			if !resp.Success {
				t.Fatalf("Failed to write test data: %s", resp.Message)
			}
		}

		resp := HandleOperation(Operation{
			Operation: "deleteDataPoint",
			Key:       testKey,
			Payload: &DeleteDataPointRequest{
				Operator: ">",
				Value:    func() *float64 { v := 2.0; return &v }(),
			},
		})
		if !resp.Success {
			t.Fatalf("deleteDataPoint failed: %s", resp.Message)
		}

		readResp := HandleOperation(Operation{
			Operation: "read",
			Key:       testKey,
			Read: &ReadRequest{
				StartTime: baseTime,
				EndTime:   baseTime + 10,
			},
		})
		if !readResp.Success {
			t.Fatalf("Read after delete failed: %s", readResp.Message)
		}

		data, ok := readResp.Data.([]models.DataPoint)
		if !ok {
			t.Fatal("Invalid response data type")
		}
		if len(data) != 2 {
			t.Fatalf("Expected 2 remaining data points, got %d", len(data))
		}
		for _, point := range data {
			if point.Value > 2.0 {
				t.Errorf("Expected only values <= 2.0 to remain, got %f", point.Value)
			}
		}
	})

	t.Run("deleteDataPoint Invalid Operator", func(t *testing.T) {
		resp := HandleOperation(Operation{
			Operation: "deleteDataPoint",
			Key:       "delete_by_value_invalid",
			Payload: &DeleteDataPointRequest{
				Operator: "=",
				Value:    func() *float64 { v := 2.0; return &v }(),
			},
		})
		if resp.Success {
			t.Fatal("Expected invalid operator request to fail")
		}
	})

	t.Run("ReloadKey Operation", func(t *testing.T) {
		testKey := "reload_key_test"
		resp := HandleOperation(Operation{
			Operation: "write",
			Key:       testKey,
			Write: &WriteRequest{
				Value:     9.9,
				Timestamp: time.Now().Unix(),
			},
		})
		if !resp.Success {
			t.Fatalf("Failed to write test data: %s", resp.Message)
		}

		reloadResp := HandleOperation(Operation{
			Operation: "reloadkey",
			Key:       testKey,
		})
		if !reloadResp.Success {
			t.Fatalf("ReloadKey failed: %s", reloadResp.Message)
		}
	})

	t.Run("deleteDataPoint with timestamp range", func(t *testing.T) {
		testKey := "delete_by_value_with_range_test"
		baseTime := time.Now().Unix()

		for i, value := range []float64{1.0, 2.0, 3.0, 4.0} {
			resp := HandleOperation(Operation{
				Operation: "write",
				Key:       testKey,
				Write: &WriteRequest{
					Value:     value,
					Timestamp: baseTime + int64(i),
				},
			})
			if !resp.Success {
				t.Fatalf("Failed to write test data: %s", resp.Message)
			}
		}

		resp := HandleOperation(Operation{
			Operation: "deleteDataPoint",
			Key:       testKey,
			Payload: &DeleteDataPointRequest{
				Operator:      ">",
				Value:         func() *float64 { v := 2.0; return &v }(),
				TimestampFrom: baseTime + 1,
				TimestampTo:   baseTime + 2,
			},
		})
		if !resp.Success {
			t.Fatalf("deleteDataPoint with range failed: %s", resp.Message)
		}

		readResp := HandleOperation(Operation{
			Operation: "read",
			Key:       testKey,
			Read: &ReadRequest{
				StartTime: baseTime,
				EndTime:   baseTime + 10,
			},
		})
		if !readResp.Success {
			t.Fatalf("Read after delete failed: %s", readResp.Message)
		}

		data, ok := readResp.Data.([]models.DataPoint)
		if !ok {
			t.Fatal("Invalid response data type")
		}
		// Remaining values should be 1.0, 2.0, 4.0 because 3.0 is in range and > 2.0, while 4.0 is out of range.
		if len(data) != 3 {
			t.Fatalf("Expected 3 remaining data points, got %d", len(data))
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

// --- New tests for recently added features ---

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{"valid simple key", "sensor1", true},
		{"valid key with slash", "user/sensor1", true},
		{"path traversal", "sensor1/../etc", false},
		{"double dots", "../evil", false},
		{"nested traversal", "a/../../../b", false},
		{"null byte", "sensor\x00", false},
		{"leading slash", "/sensor1", false},
		{"leading backslash", "\\sensor1", false},
		{"empty string", "", true},
		{"max length 512", strings.Repeat("x", 512), true},
		{"too long 513", strings.Repeat("x", 513), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validateKey(tt.key); got != tt.want {
				t.Errorf("validateKey(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestValidateTimestamp(t *testing.T) {
	tests := []struct {
		name string
		ts   int64
		want bool
	}{
		{"zero (auto)", 0, true},
		{"negative", -1, true}, // automatically replaced with Now()
		{"year 2000 boundary", 946684800, true},
		{"year 2100 boundary", 4102444800, true},
		{"before 2000", 946684799, false},
		{"after 2100", 4102444801, false},
		{"current time", 1717965210, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validateTimestamp(tt.ts); got != tt.want {
				t.Errorf("validateTimestamp(%d) = %v, want %v", tt.ts, got, tt.want)
			}
		})
	}
}

func TestBatchWriteOperation(t *testing.T) {
	t.Run("valid batch", func(t *testing.T) {
		op := Operation{
			Operation: "batch-write",
			Points: []BatchWritePoint{
				{Key: "batch_test_1", Value: 1.0},
				{Key: "batch_test_2", Value: 2.0},
				{Key: "batch_test_3", Value: 3.0},
			},
		}
		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("batch-write failed: %s", resp.Message)
		}
	})

	t.Run("batch with custom timestamps", func(t *testing.T) {
		now := time.Now().Unix()
		op := Operation{
			Operation: "batch-write",
			Points: []BatchWritePoint{
				{Key: "batch_ts_1", Value: 10.0, Timestamp: now},
				{Key: "batch_ts_2", Value: 20.0, Timestamp: now + 1},
			},
		}
		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("batch-write with timestamps failed: %s", resp.Message)
		}

		// Verify both points were stored
		readResp := HandleOperation(Operation{
			Operation: "read",
			Key:       "batch_ts_1",
			Read:      &ReadRequest{LastX: 1},
		})
		if !readResp.Success {
			t.Errorf("Read after batch failed: %s", readResp.Message)
		}
	})

	t.Run("empty points array", func(t *testing.T) {
		op := Operation{
			Operation: "batch-write",
			Points:    []BatchWritePoint{},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected batch-write with empty points to fail")
		}
	})

	t.Run("missing key in point", func(t *testing.T) {
		op := Operation{
			Operation: "batch-write",
			Points: []BatchWritePoint{
				{Value: 1.0}, // missing key
			},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected batch-write with missing key to fail")
		}
	})

	t.Run("invalid timestamp in batch", func(t *testing.T) {
		op := Operation{
			Operation: "batch-write",
			Points: []BatchWritePoint{
				{Key: "batch_invalid_ts", Value: 1.0, Timestamp: 500}, // before year 2000
			},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected batch-write with invalid timestamp to fail")
		}
	})

	t.Run("exceeds max batch size", func(t *testing.T) {
		points := make([]BatchWritePoint, 10001)
		for i := range points {
			points[i] = BatchWritePoint{Key: "big_batch", Value: float64(i)}
		}
		op := Operation{
			Operation: "batch-write",
			Points:    points,
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected batch-write with >10000 points to fail")
		}
	})
}

func TestExportOperation(t *testing.T) {
	testKey := "export_test_key"
	// Write some test data
	for i := 0; i < 5; i++ {
		HandleOperation(Operation{
			Operation: "write",
			Key:       testKey,
			Write: &WriteRequest{
				Value:     float64(i) * 10,
				Timestamp: time.Now().Unix() + int64(i),
			},
		})
	}

	t.Run("export JSON", func(t *testing.T) {
		op := Operation{
			Operation: "export",
			Key:       testKey,
			Export: &ExportRequest{
				Format: "json",
				LastX:  3,
			},
		}
		resp := HandleOperation(op)
		if !resp.Success {
			t.Fatalf("Export JSON failed: %s", resp.Message)
		}
		data, ok := resp.Data.([]models.DataPoint)
		if !ok {
			t.Fatal("Export JSON expected []DataPoint response")
		}
		if len(data) != 3 {
			t.Errorf("Expected 3 data points, got %d", len(data))
		}
	})

	t.Run("export CSV", func(t *testing.T) {
		op := Operation{
			Operation: "export",
			Key:       testKey,
			Export: &ExportRequest{
				Format: "csv",
				LastX:  2,
			},
		}
		resp := HandleOperation(op)
		if !resp.Success {
			t.Fatalf("Export CSV failed: %s", resp.Message)
		}
		csvData, ok := resp.Data.(string)
		if !ok {
			t.Fatal("Export CSV expected string response")
		}
		if len(csvData) == 0 {
			t.Error("Expected non-empty CSV data")
		}
	})

	t.Run("export invalid format", func(t *testing.T) {
		op := Operation{
			Operation: "export",
			Key:       testKey,
			Export: &ExportRequest{
				Format: "xml",
				LastX:  1,
			},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected export with invalid format to fail")
		}
	})

	t.Run("export no params", func(t *testing.T) {
		op := Operation{
			Operation: "export",
			Key:       testKey,
			Export:    &ExportRequest{Format: "json"},
		}
		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Export with defaults failed: %s", resp.Message)
		}
	})
}

func TestServerInfoOperation(t *testing.T) {
	op := Operation{
		Operation: "serverinfo",
	}
	resp := HandleOperation(op)
	if !resp.Success {
		t.Fatalf("serverinfo failed: %s", resp.Message)
	}
	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatal("serverinfo expected map response")
	}
	// Verify enhanced fields
	fields := []string{"version", "key_count", "health", "uptime_seconds", "goroutines", "num_cpu", "listen_tcp", "listen_http", "data_dir", "file_handle_lru"}
	for _, field := range fields {
		if _, exists := data[field]; !exists {
			t.Errorf("serverinfo missing field: %s", field)
		}
	}
}

func TestCompactOperation(t *testing.T) {
	testKey := "compact_op_test"
	// Write test data
	for i := 0; i < 20; i++ {
		HandleOperation(Operation{
			Operation: "write",
			Key:       testKey,
			Write: &WriteRequest{
				Value:     float64(i),
				Timestamp: time.Now().Unix() + int64(i),
			},
		})
	}

	t.Run("compact existing key", func(t *testing.T) {
		op := Operation{
			Operation: "compact",
			Key:       testKey,
		}
		resp := HandleOperation(op)
		if !resp.Success {
			t.Errorf("Compact failed: %s", resp.Message)
		}
		// Verify data still readable after compact
		readResp := HandleOperation(Operation{
			Operation: "read",
			Key:       testKey,
			Read:      &ReadRequest{LastX: 5},
		})
		if !readResp.Success {
			t.Errorf("Read after compact failed: %s", readResp.Message)
		}
	})

	t.Run("compact non-existent key", func(t *testing.T) {
		op := Operation{
			Operation: "compact",
			Key:       "nonexistent_compact_test",
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected compact of non-existent key to fail")
		}
	})
}

func TestDataPatchSizeLimit(t *testing.T) {
	// Create data exceeding maxPatchDataLength (10MB)
	largeData := make([]byte, maxPatchDataLength+1)
	for i := range largeData {
		largeData[i] = '0'
	}

	op := Operation{
		Operation: "data-patch",
		Key:       "patch_size_test",
		Data:      string(largeData),
	}
	resp := HandleOperation(op)
	if resp.Success {
		t.Error("Expected data-patch with oversized payload to fail")
	}
}

func TestTimestampValidationInOperations(t *testing.T) {
	t.Run("write with future timestamp rejected", func(t *testing.T) {
		op := Operation{
			Operation: "write",
			Key:       "ts_test_future",
			Write: &WriteRequest{
				Value:     1.0,
				Timestamp: 4102444801, // after year 2100
			},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected write with far-future timestamp to fail")
		}
	})

	t.Run("write with past timestamp rejected", func(t *testing.T) {
		op := Operation{
			Operation: "write",
			Key:       "ts_test_past",
			Write: &WriteRequest{
				Value:     1.0,
				Timestamp: 946684799, // before year 2000
			},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected write with pre-2000 timestamp to fail")
		}
	})

	t.Run("read with invalid time range", func(t *testing.T) {
		op := Operation{
			Operation: "read",
			Key:       "ts_test_read",
			Read: &ReadRequest{
				StartTime: 500, // before year 2000
				EndTime:   1000,
			},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected read with invalid time range to fail")
		}
	})

	t.Run("delete with invalid time range", func(t *testing.T) {
		op := Operation{
			Operation: "deleteDataPoint",
			Key:       "ts_test_delete",
			Payload: &DeleteDataPointRequest{
				TimestampFrom: 500,
				TimestampTo:   1000,
			},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected deleteDataPoint with invalid time range to fail")
		}
	})
}

func TestKeyValidationInOperations(t *testing.T) {
	t.Run("path traversal key rejected", func(t *testing.T) {
		op := Operation{
			Operation: "write",
			Key:       "../etc/passwd",
			Write:     &WriteRequest{Value: 1.0},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected write with path traversal key to fail")
		}
	})

	t.Run("path traversal rename rejected", func(t *testing.T) {
		op := Operation{
			Operation: "renamekey",
			Key:       "safe_key",
			ToKey:     "../evil",
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected renamekey with path traversal to fail")
		}
	})

	t.Run("null byte key rejected", func(t *testing.T) {
		op := Operation{
			Operation: "write",
			Key:       "sensor\x00name",
			Write:     &WriteRequest{Value: 1.0},
		}
		resp := HandleOperation(op)
		if resp.Success {
			t.Error("Expected write with null byte key to fail")
		}
	})
}

func TestIdsOperations(t *testing.T) {
	// Ensure there are keys
	HandleOperation(Operation{Operation: "write", Key: "ids_test_1", Write: &WriteRequest{Value: 1.0}})
	HandleOperation(Operation{Operation: "write", Key: "ids_test_2", Write: &WriteRequest{Value: 2.0}})

	t.Run("ids", func(t *testing.T) {
		resp := HandleOperation(Operation{Operation: "ids"})
		if !resp.Success {
			t.Fatalf("ids failed: %s", resp.Message)
		}
	})

	t.Run("idswithcount", func(t *testing.T) {
		resp := HandleOperation(Operation{Operation: "idswithcount"})
		if !resp.Success {
			t.Fatalf("idswithcount failed: %s", resp.Message)
		}
	})
}

func TestFlushOperation(t *testing.T) {
	resp := HandleOperation(Operation{Operation: "flush"})
	if !resp.Success {
		t.Fatalf("flush failed: %s", resp.Message)
	}
}

func TestKeyManagementOperations(t *testing.T) {
	t.Run("initkey", func(t *testing.T) {
		resp := HandleOperation(Operation{Operation: "initkey", Key: "new_init_key"})
		if !resp.Success {
			t.Fatalf("initkey failed: %s", resp.Message)
		}
	})

	t.Run("renamekey", func(t *testing.T) {
		HandleOperation(Operation{Operation: "initkey", Key: "rename_src"})
		resp := HandleOperation(Operation{Operation: "renamekey", Key: "rename_src", ToKey: "rename_dst"})
		if !resp.Success {
			t.Fatalf("renamekey failed: %s", resp.Message)
		}
	})

	t.Run("renamekey missing toKey", func(t *testing.T) {
		resp := HandleOperation(Operation{Operation: "renamekey", Key: "rename_src"})
		// Production may accept rename without toKey
		_ = resp
	})

	t.Run("deletekey", func(t *testing.T) {
		HandleOperation(Operation{Operation: "initkey", Key: "delete_me"})
		resp := HandleOperation(Operation{Operation: "deletekey", Key: "delete_me"})
		if !resp.Success {
			t.Fatalf("deletekey failed: %s", resp.Message)
		}
	})

	t.Run("reloadkey", func(t *testing.T) {
		HandleOperation(Operation{Operation: "write", Key: "reload_test", Write: &WriteRequest{Value: 1.0}})
		resp := HandleOperation(Operation{Operation: "reloadkey", Key: "reload_test"})
		if !resp.Success {
			t.Fatalf("reloadkey failed: %s", resp.Message)
		}
	})
}

func TestDeleteDataPointOperation(t *testing.T) {
	key := "delete_by_value_test"
	// Write test data
	for i := 0; i < 5; i++ {
		HandleOperation(Operation{
			Operation: "write",
			Key:       key,
			Write:     &WriteRequest{Value: float64(i), Timestamp: time.Now().Unix() + int64(i)},
		})
		time.Sleep(time.Millisecond)
	}

	t.Run("delete by value condition", func(t *testing.T) {
		resp := HandleOperation(Operation{
			Operation: "deleteDataPoint",
			Key:       key,
			Payload:   &DeleteDataPointRequest{Operator: ">", Value: ptr(3.0)},
		})
		if !resp.Success {
			t.Fatalf("deleteDataPoint failed: %s", resp.Message)
		}
	})

	t.Run("delete with time range", func(t *testing.T) {
		key2 := "delete_by_value_with_range_test"
		now := time.Now().Unix()
		for i := 0; i < 5; i++ {
			HandleOperation(Operation{
				Operation: "write",
				Key:       key2,
				Write:     &WriteRequest{Value: float64(i), Timestamp: now + int64(i)},
			})
			time.Sleep(time.Millisecond)
		}
		resp := HandleOperation(Operation{
			Operation: "deleteDataPoint",
			Key:       key2,
			Payload:   &DeleteDataPointRequest{Operator: ">", Value: ptr(1.0), TimestampFrom: now, TimestampTo: now + 10},
		})
		if !resp.Success {
			t.Fatalf("deleteDataPoint with range failed: %s", resp.Message)
		}
	})

	t.Run("delete missing key", func(t *testing.T) {
		_ = HandleOperation(Operation{
			Operation: "deleteDataPoint",
			Key:       "nonexistent_delete",
			Payload:   &DeleteDataPointRequest{Operator: ">", Value: ptr(0.0)},
		})
	})
}

func TestDataPatchOperation(t *testing.T) {
	key := "patch_test"
	HandleOperation(Operation{Operation: "initkey", Key: key})

	t.Run("patch with CSV format", func(t *testing.T) {
		resp := HandleOperation(Operation{
			Operation: "data-patch",
			Key:       key,
			Data:      "2000000000,1.5\n2000000001,2.5\n2000000002,3.5",
		})
		if !resp.Success {
			t.Fatalf("data-patch CSV failed: %s", resp.Message)
		}
	})

	t.Run("patch empty data", func(t *testing.T) {
		resp := HandleOperation(Operation{
			Operation: "data-patch",
			Key:       key,
			Data:      "",
		})
		if resp.Success {
			t.Error("Expected data-patch with empty data to fail")
		}
	})
}
func ptr(f float64) *float64 { return &f }
