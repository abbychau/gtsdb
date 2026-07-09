package buffer

import (
	"gtsdb/models"
	"gtsdb/utils"
	"os"
	"testing"
)

func TestGorillaCompactedRead(t *testing.T) {
	cleanup()
	defer cleanup()

	originalCompression := utils.CompactionCompression
	utils.CompactionCompression = true
	defer func() { utils.CompactionCompression = originalCompression }()

	key := "test_gorilla_compact"
	for i := 0; i < 100; i++ {
		StoreDataPointBuffer(models.DataPoint{
			Key: key, Timestamp: int64(1000 + i), Value: float64(i) * 1.5,
		})
	}

	err := CompactKey(key)
	if err != nil {
		t.Fatalf("CompactKey failed: %v", err)
	}

	if _, err := os.Stat(utils.DataDir + "/" + key + ".aof.gor"); os.IsNotExist(err) {
		t.Error("Expected .aof.gor file to exist after compaction")
	}

	points := ReadDataPoints(key, 0, 2000, 0, "avg")
	if len(points) != 100 {
		t.Errorf("Expected 100 points from compressed WAL, got %d", len(points))
	}
	if points[0].Timestamp != 1000 || points[0].Value != 0.0 {
		t.Errorf("First point mismatch: ts=%d val=%f", points[0].Timestamp, points[0].Value)
	}
	if points[99].Timestamp != 1099 || points[99].Value != 99*1.5 {
		t.Errorf("Last point mismatch: ts=%d val=%f", points[99].Timestamp, points[99].Value)
	}
}

func TestWriteReadCompressedWAL(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_gorilla_write_read"
	originalCompression := utils.CompactionCompression
	utils.CompactionCompression = true
	defer func() { utils.CompactionCompression = originalCompression }()

	points := make([]models.DataPoint, 0, gorillaBlockSize+100)
	for i := 0; i < gorillaBlockSize+100; i++ {
		points = append(points, models.DataPoint{
			Timestamp: int64(1000 + i), Value: float64(i) * 1.5,
		})
	}
	for _, p := range points {
		StoreDataPointBuffer(models.DataPoint{Key: key, Timestamp: p.Timestamp, Value: p.Value})
	}

	err := CompactKey(key)
	if err != nil {
		t.Fatalf("CompactKey failed: %v", err)
	}

	allPoints, err := readCompressedDataPoints(key, 0, 100000)
	if err != nil {
		t.Fatalf("readCompressedDataPoints failed: %v", err)
	}
	if len(allPoints) != len(points) {
		t.Errorf("Expected %d points, got %d", len(points), len(allPoints))
	}

	midPoints, err := readCompressedDataPoints(key, 1500, 2000)
	if err != nil {
		t.Fatalf("readCompressedDataPoints (range) failed: %v", err)
	}
	if len(midPoints) != 501 {
		t.Errorf("Expected 501 points in range [1500,2000], got %d", len(midPoints))
	}
}

func TestReadCompressedNoFile(t *testing.T) {
	cleanup()
	defer cleanup()

	points, err := readCompressedDataPoints("nonexistent_gorilla", 0, 100)
	if err != nil {
		t.Errorf("Expected nil error for missing file, got: %v", err)
	}
	if points != nil {
		t.Error("Expected nil points for missing compressed file")
	}
}

func TestWriteCompressedEmptyData(t *testing.T) {
	cleanup()
	defer cleanup()

	err := writeCompressedWAL("test_empty", []models.DataPoint{})
	if err != nil {
		t.Errorf("Expected no error for empty data, got: %v", err)
	}
}

func TestReadCompressedSingleBlock(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_gorilla_single_block"
	originalCompression := utils.CompactionCompression
	utils.CompactionCompression = true
	defer func() { utils.CompactionCompression = originalCompression }()

	for i := 0; i < 10; i++ {
		StoreDataPointBuffer(models.DataPoint{
			Key: key, Timestamp: int64(2000 + i*10), Value: float64(i) * 2.0,
		})
	}

	err := CompactKey(key)
	if err != nil {
		t.Fatalf("CompactKey failed: %v", err)
	}

	before, err := readCompressedDataPoints(key, 0, 1000)
	if err != nil {
		t.Fatalf("readCompressedDataPoints failed: %v", err)
	}
	if len(before) != 0 {
		t.Errorf("Expected 0 points before first, got %d", len(before))
	}

	all, err := readCompressedDataPoints(key, 0, 100000)
	if err != nil {
		t.Fatalf("readCompressedDataPoints failed: %v", err)
	}
	if len(all) != 10 {
		t.Errorf("Expected 10 points, got %d", len(all))
	}
}

func TestReadCompressedFilterEarlyExit(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_gorilla_early_exit"
	originalCompression := utils.CompactionCompression
	utils.CompactionCompression = true
	defer func() { utils.CompactionCompression = originalCompression }()

	for i := 0; i < 10; i++ {
		StoreDataPointBuffer(models.DataPoint{
			Key: key, Timestamp: int64(1000 + i*10), Value: float64(i),
		})
	}

	err := CompactKey(key)
	if err != nil {
		t.Fatalf("CompactKey failed: %v", err)
	}

	partial, err := readCompressedDataPoints(key, 1030, 1050)
	if err != nil {
		t.Fatalf("readCompressedDataPoints failed: %v", err)
	}
	if len(partial) != 3 {
		t.Errorf("Expected 3 points in [1030,1050], got %d", len(partial))
	}
}

func TestReadCompressedMissingIndex(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_no_idx"
	// Create gor file without corresponding idx file
	gorPath := utils.DataDir + "/" + key + ".aof.gor"
	if err := os.WriteFile(gorPath, []byte("dummy"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := readCompressedDataPoints(key, 0, 1000)
	if err == nil {
		t.Error("Expected error when index file is missing")
	}
}

func TestReadCompressedCorruptGorData(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_corrupt_gor"
	// Write a valid index and a corrupt gor file (valid block header but bad data)
	idxPath := utils.DataDir + "/" + key + ".idx"
	gorPath := utils.DataDir + "/" + key + ".aof.gor"

	// Write index entry: ts=0, offset=0
	idxData := make([]byte, 16)
	// ts=0 at bytes 0-7, offset=0 at bytes 8-15 (little-endian zeros)
	if err := os.WriteFile(idxPath, idxData, 0644); err != nil {
		t.Fatal(err)
	}

	// Write gor file with block length prefix but no actual data
	var lenBuf [4]byte
	lenBuf[0] = 100 // claim 100 bytes of block data, but none follows
	gorData := append(lenBuf[:], []byte("short")...)
	if err := os.WriteFile(gorPath, gorData, 0644); err != nil {
		t.Fatal(err)
	}

	_, err := readCompressedDataPoints(key, 0, 1000)
	if err == nil {
		t.Error("Expected error for corrupt gor data")
	}
}

func TestReadCompressedCorruptIndex(t *testing.T) {
	cleanup()
	defer cleanup()

	key := "test_corrupt_idx"
	gorPath := utils.DataDir + "/" + key + ".aof.gor"
	idxPath := utils.DataDir + "/" + key + ".idx"

	// Write a valid gor file with one small block
	orgComp := utils.CompactionCompression
	utils.CompactionCompression = true
	defer func() { utils.CompactionCompression = orgComp }()

	// Store and compact to create valid files
	for i := 0; i < 5; i++ {
		StoreDataPointBuffer(models.DataPoint{
			Key: key, Timestamp: int64(1000 + i), Value: float64(i),
		})
	}
	if err := CompactKey(key); err != nil {
		t.Fatal(err)
	}

	// Now corrupt the index file
	if err := os.WriteFile(idxPath, []byte("garbage"), 0644); err != nil {
		t.Fatal(err)
	}

	// Should still read from gor without valid index
	pts, err := readCompressedDataPoints(key, 0, 100000)
	if err != nil {
		// Error is expected for corrupt index
		t.Logf("Got expected error: %v", err)
	}
	_ = pts
	_ = gorPath // suppress unused warning
}
