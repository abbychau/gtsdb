package buffer

import (
	"gtsdb/models"
	"testing"
	"time"
)

func TestGorillaRoundTrip(t *testing.T) {
	// Create data with regular intervals (best case for Gorilla)
	points := make([]models.DataPoint, 100)
	baseTs := time.Now().Unix()
	for i := 0; i < 100; i++ {
		points[i] = models.DataPoint{
			Timestamp: baseTs + int64(i),
			Value:     float64(i) * 1.5,
		}
	}

	// Encode
	compressed, err := EncodeBlock(points)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	// Decode
	decoded, err := DecodeBlock(compressed)
	if err != nil {
		t.Fatalf("DecodeBlock failed: %v", err)
	}

	if len(decoded) != len(points) {
		t.Fatalf("Expected %d points, got %d", len(points), len(decoded))
	}

	for i := range points {
		if decoded[i].Timestamp != points[i].Timestamp {
			t.Errorf("Timestamp mismatch at %d: expected %d, got %d", i, points[i].Timestamp, decoded[i].Timestamp)
		}
		if decoded[i].Value != points[i].Value {
			t.Errorf("Value mismatch at %d: expected %f, got %f", i, points[i].Value, decoded[i].Value)
		}
	}

	// Verify compression ratio (should be much smaller than 16*100 = 1600 bytes)
	rawSize := len(points) * 16
	t.Logf("Raw size: %d bytes, Compressed: %d bytes (%.1fx)", rawSize, len(compressed), float64(rawSize)/float64(len(compressed)))
	if len(compressed) >= rawSize {
		t.Errorf("Compression should reduce size: raw=%d, compressed=%d", rawSize, len(compressed))
	}
}

func TestGorillaVariableInterval(t *testing.T) {
	// Test with irregular timestamps and values
	points := []models.DataPoint{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 1005, Value: 1.1},
		{Timestamp: 1010, Value: 1.2},
		{Timestamp: 2000, Value: 5.0}, // large gap
		{Timestamp: 2001, Value: 5.1},
		{Timestamp: 2002, Value: 5.2},
	}

	compressed, err := EncodeBlock(points)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	decoded, err := DecodeBlock(compressed)
	if err != nil {
		t.Fatalf("DecodeBlock failed: %v", err)
	}

	if len(decoded) != len(points) {
		t.Fatalf("Expected %d points, got %d", len(points), len(decoded))
	}

	for i := range points {
		if decoded[i].Timestamp != points[i].Timestamp {
			t.Errorf("Timestamp mismatch at %d", i)
		}
		if decoded[i].Value != points[i].Value {
			t.Errorf("Value mismatch at %d", i)
		}
	}
}

func TestGorillaSameValue(t *testing.T) {
	// Test with constant values (best case for XOR compression)
	points := make([]models.DataPoint, 50)
	for i := 0; i < 50; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i),
			Value:     42.0, // constant value
		}
	}

	compressed, err := EncodeBlock(points)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	decoded, err := DecodeBlock(compressed)
	if err != nil {
		t.Fatalf("DecodeBlock failed: %v", err)
	}

	for _, p := range decoded {
		if p.Value != 42.0 {
			t.Errorf("Expected constant 42.0, got %f", p.Value)
		}
	}
}

func TestGorillaEmpty(t *testing.T) {
	_, err := EncodeBlock([]models.DataPoint{})
	if err == nil {
		t.Error("Expected error for empty block")
	}
}

func TestGorillaAllDoDBranches(t *testing.T) {
	// Test all 5 delta-of-delta encoding branches:
	// '0'  = DoD=0         (1 bit)
	// '10' = DoD in [-63,64]   (9 bits)
	// '110' = DoD in [-255,256] (12 bits)
	// '1110' = DoD in [-2047,2048] (16 bits)
	// '1111' = full 32-bit DoD

	points := []models.DataPoint{
		{Timestamp: 1000, Value: 1.0},  // anchor
		{Timestamp: 1001, Value: 1.1},  // DoD=0 branch (regular +1 interval)
		{Timestamp: 1002, Value: 1.2},  // DoD=0 again
		{Timestamp: 1100, Value: 1.3},  // '10' branch (delta=98, DoD=97 > 64 → hits next branch? No: 98-1=97, DoD=97 → '110' branch
		{Timestamp: 1200, Value: 1.4},  // '10' branch: delta=100, DoD=2 → fits in '10'?
		{Timestamp: 1500, Value: 1.5},  // '110' branch: delta=300, DoD=200 → fits '110' [-255,256]
		{Timestamp: 2500, Value: 1.6},  // '110' branch: delta=1000, DoD=700 → NO, fits '1110' [-2047,2048]? 700 yes
		{Timestamp: 10000, Value: 1.7}, // '1110' branch: delta=7500, DoD=6500 → NO, '1111' full 32-bit
		{Timestamp: 10001, Value: 1.8}, // back to DoD=0: delta=1, DoD=-7499 → '1111'? Wait, this is complex
	}

	compressed, err := EncodeBlock(points)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	decoded, err := DecodeBlock(compressed)
	if err != nil {
		t.Fatalf("DecodeBlock failed: %v", err)
	}

	if len(decoded) != len(points) {
		t.Fatalf("Expected %d points, got %d", len(points), len(decoded))
	}
	for i := range points {
		if decoded[i].Timestamp != points[i].Timestamp {
			t.Errorf("Timestamp mismatch at %d: expected %d, got %d", i, points[i].Timestamp, decoded[i].Timestamp)
		}
	}

	t.Logf("All DoD branches covered: 9 points: %d bytes → %d bytes (%.1fx)",
		len(points)*16, len(compressed), float64(len(points)*16)/float64(len(compressed)))
}

func TestGorillaLargeDoD(t *testing.T) {
	// Specifically test '1110' (12-bit) and '1111' (32-bit) DoD branches
	points := []models.DataPoint{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 4000, Value: 2.0}, // delta=3000, DoD=3000 (>2047 → '1111')
		{Timestamp: 6000, Value: 3.0}, // delta=2000, DoD=-1000 (<2047 but >255 → '1110')
		{Timestamp: 6100, Value: 4.0}, // delta=100, DoD=-1900 → '1110'
	}

	compressed, err := EncodeBlock(points)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	decoded, err := DecodeBlock(compressed)
	if err != nil {
		t.Fatalf("DecodeBlock failed: %v", err)
	}

	if len(decoded) != len(points) {
		t.Fatalf("Expected %d points, got %d", len(points), len(decoded))
	}
	for i := range points {
		if decoded[i].Timestamp != points[i].Timestamp {
			t.Errorf("Timestamp mismatch at %d", i)
		}
	}
}

func TestGorillaCLZEdgeCases(t *testing.T) {
	// Test clz with values that exercise all bit ranges
	// clz branches: 0xFFFFFFFF00000000, 0xFFFF000000000000, 0xFF00000000000000,
	//               0xF000000000000000, 0xC000000000000000, 0x8000000000000000
	// These get triggered when XOR has varying numbers of leading zeros

	// Use values that produce specific XOR patterns
	points := []models.DataPoint{
		{Timestamp: 1000, Value: 0.0},      // 0x0000000000000000
		{Timestamp: 1001, Value: 1.0},      // 0x3FF0000000000000 → XOR leading zeros: 2
		{Timestamp: 1002, Value: 1.0e-300}, // very small → XOR has many leading zeros
		{Timestamp: 1003, Value: 1.0e300},  // very large → different bit pattern
		{Timestamp: 1004, Value: -1.0},     // 0xBFF0000000000000 → bit 63 set
	}

	compressed, err := EncodeBlock(points)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	decoded, err := DecodeBlock(compressed)
	if err != nil {
		t.Fatalf("DecodeBlock failed: %v", err)
	}

	for i := range points {
		if decoded[i].Value != points[i].Value {
			t.Errorf("Value mismatch at %d: expected %g, got %g", i, points[i].Value, decoded[i].Value)
		}
	}
}
