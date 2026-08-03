package buffer

import (
	"gtsdb/models"
	"os"
	"testing"
)

func BenchmarkGorillaEncode(b *testing.B) {
	points := make([]models.DataPoint, 5000)
	for i := 0; i < 5000; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i),
			Value:     float64(i) * 0.5,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeBlock(points); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGorillaDecode(b *testing.B) {
	points := make([]models.DataPoint, 5000)
	for i := 0; i < 5000; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i),
			Value:     float64(i) * 0.5,
		}
	}
	compressed, _ := EncodeBlock(points)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeBlock(compressed); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGorillaEncodeVariable(b *testing.B) {
	points := make([]models.DataPoint, 5000)
	for i := 0; i < 5000; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i*5),                   // irregular intervals
			Value:     float64(i)*0.5 + float64(i%10)*0.01, // slightly varying
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeBlock(points); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGorillaDecodeVariable(b *testing.B) {
	points := make([]models.DataPoint, 5000)
	for i := 0; i < 5000; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i*5),
			Value:     float64(i)*0.5 + float64(i%10)*0.01,
		}
	}
	compressed, _ := EncodeBlock(points)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeBlock(compressed); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRawWrite(b *testing.B) {
	points := make([]models.DataPoint, 5000)
	for i := 0; i < 5000; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i),
			Value:     float64(i) * 0.5,
		}
	}
	// Simulate raw 16-byte write (no I/O)
	buf := make([]byte, 5000*16)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j, p := range points {
			offset := j * 16
			buf[offset] = byte(p.Timestamp)
			buf[offset+8] = byte(p.Value)
		}
	}
}

func BenchmarkGorillaSize(b *testing.B) {
	points := make([]models.DataPoint, 5000)
	for i := 0; i < 5000; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i),
			Value:     float64(i) * 0.5,
		}
	}
	compressed, _ := EncodeBlock(points)
	rawSize := len(points) * 16

	b.ReportMetric(float64(rawSize), "raw-bytes")
	b.ReportMetric(float64(len(compressed)), "gor-bytes")
	b.ReportMetric(float64(rawSize)/float64(len(compressed)), "ratio")
}

func BenchmarkWriteRecordDisk(b *testing.B) {
	f, _ := os.CreateTemp("", "bench_write_*")
	defer os.Remove(f.Name())
	defer f.Close()

	timestamp := int64(1234567)
	value := 42.5

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := writeRecord(f, timestamp, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGorillaFullCycle(b *testing.B) {
	points := make([]models.DataPoint, 5000)
	for i := 0; i < 5000; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i),
			Value:     float64(i) * 0.5,
		}
	}

	f, _ := os.CreateTemp("", "bench_gor_*")
	defer os.Remove(f.Name())
	defer f.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compressed, _ := EncodeBlock(points)
		if _, err := f.Write(compressed); err != nil {
			b.Fatal(err)
		}
		if _, err := f.Seek(0, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRawFullCycle(b *testing.B) {
	points := make([]models.DataPoint, 5000)
	for i := 0; i < 5000; i++ {
		points[i] = models.DataPoint{
			Timestamp: int64(1000 + i),
			Value:     float64(i) * 0.5,
		}
	}

	f, _ := os.CreateTemp("", "bench_raw_*")
	defer os.Remove(f.Name())
	defer f.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, p := range points {
			if err := writeRecord(f, p.Timestamp, p.Value); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := f.Seek(0, 0); err != nil {
			b.Fatal(err)
		}
	}
}
