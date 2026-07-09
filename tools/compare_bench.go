//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var client = &http.Client{
	Transport: &http.Transport{MaxIdleConnsPerHost: 100},
	Timeout:   30 * time.Second,
}

func post(url, body string) string {
	resp, err := client.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		panic(fmt.Sprintf("POST %s: %v", url, err))
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return string(data)
}

func postPlain(url, body string) string {
	resp, err := client.Post(url, "text/plain", strings.NewReader(body))
	if err != nil {
		panic(fmt.Sprintf("POST %s: %v", url, err))
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return string(data)
}

func main() {
	const totalPoints = 10000
	const sensors = 10
	const pointsPerSensor = totalPoints / sensors
	const token = "bench-token-123"

	// ============================================================
	// GTSDB BENCHMARK (HTTP keep-alive, port 5556)
	// ============================================================
	fmt.Println("========================================")
	fmt.Println("=== GTSDB (HTTP keep-alive) ===")
	fmt.Println("========================================")

	gtsdbSend := func(cmd string) string {
		return post("http://localhost:5556/", cmd)
	}

	// Ensure keys
	for i := 0; i < sensors; i++ {
		gtsdbSend(fmt.Sprintf(`{"operation":"initkey","key":"gtsdb_sensor_%d"}`, i))
	}
	time.Sleep(50 * time.Millisecond)

	// Pre-load via batch-write
	fmt.Println("GTSDB pre-loading...")
	for i := 0; i < sensors; i++ {
		var parts []string
		for j := 0; j < pointsPerSensor; j++ {
			parts = append(parts, fmt.Sprintf(`{"key":"gtsdb_sensor_%d","value":%f,"timestamp":%d}`, i, float64(j)*1.5, 1700000000+int64(j)))
		}
		gtsdbSend(fmt.Sprintf(`{"operation":"batch-write","points":[%s]}`, strings.Join(parts, ",")))
	}

	// Write benchmark
	fmt.Println("\nGTSDB Write (10k single writes)...")
	gtsdbWriteStart := time.Now()
	for i := 0; i < sensors; i++ {
		for j := 0; j < pointsPerSensor; j++ {
			cmd := fmt.Sprintf(`{"operation":"write","key":"gtsdb_sensor_%d","write":{"timestamp":%d,"value":%f}}`, i, 1800000000+int64(j), float64(j)*1.5)
			gtsdbSend(cmd)
		}
	}
	gtsdbWrite := time.Since(gtsdbWriteStart)

	// Read benchmark
	fmt.Println("GTSDB Read (10k reads)...")
	gtsdbReadStart := time.Now()
	for i := 0; i < sensors; i++ {
		for j := 0; j < pointsPerSensor; j++ {
			cmd := fmt.Sprintf(`{"operation":"read","key":"gtsdb_sensor_%d","read":{"lastx":1}}`, i)
			gtsdbSend(cmd)
		}
	}
	gtsdbRead := time.Since(gtsdbReadStart)

	// Batch write benchmark
	fmt.Println("GTSDB Batch (100x100)...")
	gtsdbBatchStart := time.Now()
	for b := 0; b < 100; b++ {
		var parts []string
		for p := 0; p < 100; p++ {
			sensorIdx := p % sensors
			parts = append(parts, fmt.Sprintf(`{"key":"gtsdb_sensor_%d","value":%f,"timestamp":%d}`, sensorIdx, float64(p)*2.5, 1900000000+int64(b*100+p)))
		}
		gtsdbSend(fmt.Sprintf(`{"operation":"batch-write","points":[%s]}`, strings.Join(parts, ",")))
	}
	gtsdbBatch := time.Since(gtsdbBatchStart)

	// Cleanup
	for i := 0; i < sensors; i++ {
		gtsdbSend(fmt.Sprintf(`{"operation":"deletekey","key":"gtsdb_sensor_%d"}`, i))
	}

	// ============================================================
	// INFLUXDB BENCHMARK (HTTP keep-alive, port 8086)
	// ============================================================
	fmt.Println("\n========================================")
	fmt.Println("=== InfluxDB 2.9.1 (HTTP keep-alive) ===")
	fmt.Println("========================================")

	influxWriteURL := "http://localhost:8086/api/v2/write?org=bench&bucket=bench"
	influxQueryURL := "http://localhost:8086/api/v2/query?org=bench"
	authHeader := "Token " + token

	influxWrite := func(line string) {
		req, _ := http.NewRequest("POST", influxWriteURL, strings.NewReader(line))
		req.Header.Set("Authorization", authHeader)
		req.Header.Set("Content-Type", "text/plain")
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			panic(fmt.Sprintf("influx write %d: %s", resp.StatusCode, string(body)))
		}
	}

	influxQuery := func(flux string) string {
		req, _ := http.NewRequest("POST", influxQueryURL, strings.NewReader(flux))
		req.Header.Set("Authorization", authHeader)
		req.Header.Set("Content-Type", "application/vnd.flux")
		req.Header.Set("Accept", "application/csv")
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	// Pre-load via batch (line protocol, 1000 lines per POST)
	fmt.Println("InfluxDB pre-loading...")
	for i := 0; i < sensors; i++ {
		var lines []string
		for j := 0; j < pointsPerSensor; j++ {
			lines = append(lines, fmt.Sprintf("sensor,key=sensor%d value=%f %d", i, float64(j)*1.5, 1700000000+int64(j)))
		}
		influxWrite(strings.Join(lines, "\n"))
	}

	// Write benchmark
	fmt.Println("InfluxDB Write (10k single writes)...")
	influxWriteStart := time.Now()
	for i := 0; i < sensors; i++ {
		for j := 0; j < pointsPerSensor; j++ {
			line := fmt.Sprintf("sensor,key=sensor%d value=%f %d", i, float64(j)*1.5, 1800000000+int64(j))
			influxWrite(line)
		}
	}
	influxWriteElapsed := time.Since(influxWriteStart)

	// Read benchmark (last 1 record per sensor)
	fmt.Println("InfluxDB Read (10k queries)...")
	influxReadStart := time.Now()
	for i := 0; i < sensors; i++ {
		for j := 0; j < pointsPerSensor; j++ {
			flux := fmt.Sprintf(`from(bucket:"bench") |> range(start: 0) |> filter(fn: (r) => r._measurement == "sensor" and r.key == "sensor%d") |> last()`, i)
			influxQuery(flux)
		}
	}
	influxReadElapsed := time.Since(influxReadStart)

	// Batch write benchmark (100 batches of 100 lines)
	fmt.Println("InfluxDB Batch (100x100)...")
	influxBatchStart := time.Now()
	for b := 0; b < 100; b++ {
		var lines []string
		for p := 0; p < 100; p++ {
			sensorIdx := p % sensors
			lines = append(lines, fmt.Sprintf("sensor,key=sensor%d value=%f %d", sensorIdx, float64(p)*2.5, 1900000000+int64(b*100+p)))
		}
		influxWrite(strings.Join(lines, "\n"))
	}
	influxBatchElapsed := time.Since(influxBatchStart)

	// ============================================================
	// RESULTS
	// ============================================================
	fmt.Println("\n========================================")
	fmt.Println("=== RESULTS (10,000 ops each, HTTP keep-alive) ===")
	fmt.Println("========================================")
	fmt.Printf("%-15s %12s %12s %12s\n", "", "Write", "Read", "Batch")
	fmt.Printf("%-15s %10.2f ms %10.2f ms %10.2f ms\n", "GTSDB",
		float64(gtsdbWrite.Microseconds())/1000.0,
		float64(gtsdbRead.Microseconds())/1000.0,
		float64(gtsdbBatch.Microseconds())/1000.0)
	fmt.Printf("%-15s %10.2f ms %10.2f ms %10.2f ms\n", "InfluxDB 2.9.1",
		float64(influxWriteElapsed.Microseconds())/1000.0,
		float64(influxReadElapsed.Microseconds())/1000.0,
		float64(influxBatchElapsed.Microseconds())/1000.0)
	fmt.Printf("\n%-15s %10.1fx %10.1fx %10.1fx\n", "Ratio (GTSDB vs)",
		float64(influxWriteElapsed)/float64(gtsdbWrite),
		float64(influxReadElapsed)/float64(gtsdbRead),
		float64(influxBatchElapsed)/float64(gtsdbBatch))

	fmt.Println("\n=== PAGE DATA ===")
	fmt.Printf("writeData:      GTSDB %.2f / InfluxDB %.2f\n",
		float64(gtsdbWrite.Microseconds())/1000.0,
		float64(influxWriteElapsed.Microseconds())/1000.0)
	fmt.Printf("readData:       GTSDB %.2f / InfluxDB %.2f\n",
		float64(gtsdbRead.Microseconds())/1000.0,
		float64(influxReadElapsed.Microseconds())/1000.0)
	fmt.Printf("multiWriteData: GTSDB %.2f / InfluxDB %.2f\n",
		float64(gtsdbBatch.Microseconds())/1000.0,
		float64(influxBatchElapsed.Microseconds())/1000.0)
}
