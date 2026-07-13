package handlers

import (
	"bytes"
	"fmt"
	"gtsdb/models"
	"testing"

	json "github.com/bytedance/sonic"
	velox "github.com/velox-io/json"
)

// Verify velox produces identical JSON to sonic for our types
func TestVeloxProducesSameOutput(t *testing.T) {
	// Test 1: Response with MultiData (map iteration order may differ, use semantic compare)
	resp := makeMultiDataResponse()
	sonicB, err := json.Marshal(resp)
	if err != nil {
		t.Fatal("sonic:", err)
	}
	veloxB, err := velox.Marshal(resp)
	if err != nil {
		t.Fatal("velox:", err)
	}

	// Unmarshal both back and compare structurally (map key order differs)
	var sonicResp, veloxResp Response
	if err := json.Unmarshal(sonicB, &sonicResp); err != nil {
		t.Fatal("unmarshal sonic:", err)
	}
	if err := json.Unmarshal(veloxB, &veloxResp); err != nil {
		t.Fatal("unmarshal velox:", err)
	}
	if len(sonicResp.MultiData) != len(veloxResp.MultiData) {
		t.Errorf("MultiData length mismatch: %d vs %d", len(sonicResp.MultiData), len(veloxResp.MultiData))
	}
	for k, sonicPts := range sonicResp.MultiData {
		veloxPts, ok := veloxResp.MultiData[k]
		if !ok {
			t.Errorf("MultiData missing key %q in velox", k)
			continue
		}
		if len(sonicPts) != len(veloxPts) {
			t.Errorf("MultiData[%q] length mismatch: %d vs %d", k, len(sonicPts), len(veloxPts))
			continue
		}
		for i := range sonicPts {
			if sonicPts[i] != veloxPts[i] {
				t.Errorf("MultiData[%q][%d] mismatch: %+v vs %+v", k, i, sonicPts[i], veloxPts[i])
			}
		}
	}

	// Test 2: DataPoint slice
	pts := makeDataPointsSlice(100)
	sonicB, err = json.Marshal(pts)
	if err != nil {
		t.Fatal("sonic:", err)
	}
	veloxB, err = velox.Marshal(pts)
	if err != nil {
		t.Fatal("velox:", err)
	}
	if !bytes.Equal(sonicB, veloxB) {
		t.Errorf("DataPoints mismatch:\n  sonic: %s\n  velox: %s", sonicB[:min(len(sonicB), 200)], veloxB[:min(len(veloxB), 200)])
	}

	// Test 3: Single DataPoint
	dp := models.DataPoint{Timestamp: 1717965210, Value: 42.5}
	sonicB, err = json.Marshal(dp)
	if err != nil {
		t.Fatal("sonic:", err)
	}
	veloxB, err = velox.Marshal(dp)
	if err != nil {
		t.Fatal("velox:", err)
	}
	if !bytes.Equal(sonicB, veloxB) {
		t.Errorf("Single DataPoint mismatch:\n  sonic: %s\n  velox: %s", string(sonicB), string(veloxB))
	}

	// Test 4: Response without MultiData (fallback path)
	resp2 := Response{Success: true, Message: "ok", Data: []string{"a", "b"}}
	sonicB, err = json.Marshal(resp2)
	if err != nil {
		t.Fatal("sonic:", err)
	}
	veloxB, err = velox.Marshal(resp2)
	if err != nil {
		t.Fatal("velox:", err)
	}
	if !bytes.Equal(sonicB, veloxB) {
		t.Errorf("Simple response mismatch:\n  sonic: %s\n  velox: %s", string(sonicB), string(veloxB))
	}

	t.Log("✓ All velox outputs match sonic")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Generate test data: Response with MultiData (5 keys × 5000 points each)
func makeMultiDataResponse() Response {
	multi := make(map[string][]models.DataPoint)
	ts := int64(1717965210)
	for k := 0; k < 5; k++ {
		key := fmt.Sprintf("sensor_%d", k)
		pts := make([]models.DataPoint, 5000)
		for i := 0; i < 5000; i++ {
			pts[i] = models.DataPoint{
				Timestamp: ts + int64(i),
				Value:     float64(i) * 1.5,
			}
		}
		multi[key] = pts
	}
	return Response{
		Success:   true,
		MultiData: multi,
	}
}

// Generate test data: slice of DataPoint (for batch write response)
func makeDataPointsSlice(n int) []models.DataPoint {
	pts := make([]models.DataPoint, n)
	ts := int64(1717965210)
	for i := 0; i < n; i++ {
		pts[i] = models.DataPoint{
			Timestamp: ts + int64(i),
			Value:     float64(i) * 1.5,
		}
	}
	return pts
}

// Benchmark: Sonic MarshalJSON (custom MarshalJSON with fast path)
func BenchmarkSonicMarshalMultiData(b *testing.B) {
	resp := makeMultiDataResponse()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(resp)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Velox MarshalJSON (custom MarshalJSON with fast path)
func BenchmarkVeloxMarshalMultiData(b *testing.B) {
	resp := makeMultiDataResponse()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := velox.Marshal(resp)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Sonic marshal DataPoint slice directly
func BenchmarkSonicMarshalDataPoints(b *testing.B) {
	pts := makeDataPointsSlice(5000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(pts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Velox marshal DataPoint slice directly
func BenchmarkVeloxMarshalDataPoints(b *testing.B) {
	pts := makeDataPointsSlice(5000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := velox.Marshal(pts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Sonic marshal single DataPoint
func BenchmarkSonicMarshalSingleDP(b *testing.B) {
	dp := models.DataPoint{Timestamp: 1717965210, Value: 42.5}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(dp)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Velox marshal single DataPoint
func BenchmarkVeloxMarshalSingleDP(b *testing.B) {
	dp := models.DataPoint{Timestamp: 1717965210, Value: 42.5}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := velox.Marshal(dp)
		if err != nil {
			b.Fatal(err)
		}
	}
}
