//go:build ignore
// +build ignore

package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var httpClient = &http.Client{
	Transport: &http.Transport{MaxIdleConnsPerHost: 100},
	Timeout:   30 * time.Second,
}

func main() {
	const totalPoints = 10000
	const sensors = 10
	const pointsPerSensor = totalPoints / sensors
	const token = "bench-token-123"

	fmt.Println("========================================")
	fmt.Println("  GTSDB vs InfluxDB 2.9.1 Benchmark")
	fmt.Println("  Method: GTSDB=TCP reuse, InfluxDB=HTTP keep-alive")
	fmt.Println("  i7-13700KF / Windows / amd64")
	fmt.Println("========================================")

	// ============================================================
	// GTSDB via TCP
	// ============================================================
	fmt.Println("\n--- GTSDB (TCP) ---")

	conn, err := net.Dial("tcp", ":5555")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	gtsdbSend := func(cmd string) string {
		fmt.Fprint(conn, cmd+"\n")
		resp, _ := reader.ReadString('\n')
		return strings.TrimSpace(resp)
	}

	// Init keys
	for i := 0; i < sensors; i++ {
		gtsdbSend(fmt.Sprintf(`{"operation":"initkey","key":"bench_sensor_%d"}`, i))
	}
	time.Sleep(50 * time.Millisecond)

	// Pre-load
	fmt.Println("Pre-loading...")
	for i := 0; i < sensors; i++ {
		var parts []string
		for j := 0; j < pointsPerSensor; j++ {
			parts = append(parts, fmt.Sprintf(`{"key":"bench_sensor_%d","value":%f,"timestamp":%d}`, i, float64(j)*1.5, 1700000000+int64(j)))
		}
		gtsdbSend(fmt.Sprintf(`{"operation":"batch-write","points":[%s]}`, strings.Join(parts, ",")))
	}

	// === WRITE: 10,000 sequential single writes ===
	fmt.Println("Write (10k sequential)...")
	gtsdbWriteStart := time.Now()
	for i := 0; i < sensors; i++ {
		for j := 0; j < pointsPerSensor; j++ {
			cmd := fmt.Sprintf(`{"operation":"write","key":"bench_sensor_%d","write":{"timestamp":%d,"value":%f}}`, i, 1800000000+int64(j), float64(j)*1.5)
			gtsdbSend(cmd)
		}
	}
	gtsdbWrite := time.Since(gtsdbWriteStart)

	// === READ: single query (lastx=100), like old benchmark ===
	fmt.Println("Read (1 query, lastx=100)...")
	gtsdbReadStart := time.Now()
	cmd := fmt.Sprintf(`{"operation":"read","key":"bench_sensor_0","read":{"lastx":100}}`)
	gtsdbSend(cmd)
	gtsdbRead := time.Since(gtsdbReadStart)

	// === MULTI-WRITE: 10 goroutines parallel, each own TCP connection ===
	fmt.Println("Multi-Write (10 goroutines, parallel TCP)...")
	var gtsdbMultiSuccess atomic.Uint64
	gtsdbMultiStart := time.Now()
	var wg sync.WaitGroup
	wg.Add(sensors)
	for i := 0; i < sensors; i++ {
		go func(sid int) {
			defer wg.Done()
			c, err := net.Dial("tcp", ":5555")
			if err != nil {
				return
			}
			defer c.Close()
			r := bufio.NewReader(c)
			send := func(cmd string) {
				fmt.Fprint(c, cmd+"\n")
				r.ReadString('\n')
			}
			for j := 0; j < pointsPerSensor; j++ {
				ts := 1900000000 + int64(sid*pointsPerSensor+j)
				val := float64(sid*pointsPerSensor + j)
				send(fmt.Sprintf(`{"operation":"write","key":"bench_sensor_%d","write":{"timestamp":%d,"value":%f}}`, sid, ts, val))
				gtsdbMultiSuccess.Add(1)
			}
		}(i)
	}
	wg.Wait()
	gtsdbMulti := time.Since(gtsdbMultiStart)

	// Cleanup
	for i := 0; i < sensors; i++ {
		gtsdbSend(fmt.Sprintf(`{"operation":"deletekey","key":"bench_sensor_%d"}`, i))
	}

	// ============================================================
	// InfluxDB via HTTP
	// ============================================================
	fmt.Println("\n--- InfluxDB 2.9.1 (HTTP) ---")

	influxWriteURL := "http://localhost:8086/api/v2/write?org=bench&bucket=bench"
	influxQueryURL := "http://localhost:8086/api/v2/query?org=bench"

	influxWrite := func(line string) string {
		req, _ := http.NewRequest("POST", influxWriteURL, strings.NewReader(line))
		req.Header.Set("Authorization", "Token "+token)
		req.Header.Set("Content-Type", "text/plain")
		resp, err := httpClient.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	influxQuery := func(flux string) string {
		req, _ := http.NewRequest("POST", influxQueryURL, strings.NewReader(flux))
		req.Header.Set("Authorization", "Token "+token)
		req.Header.Set("Content-Type", "application/vnd.flux")
		req.Header.Set("Accept", "application/csv")
		resp, err := httpClient.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	// Pre-load
	fmt.Println("Pre-loading...")
	for i := 0; i < sensors; i++ {
		var lines []string
		for j := 0; j < pointsPerSensor; j++ {
			lines = append(lines, fmt.Sprintf("sensor,key=sensor%d value=%f %d", i, float64(j)*1.5, 1700000000+int64(j)))
		}
		influxWrite(strings.Join(lines, "\n"))
	}

	// === WRITE: 10,000 sequential single writes ===
	fmt.Println("Write (10k sequential)...")
	influxWriteStart := time.Now()
	for i := 0; i < sensors; i++ {
		for j := 0; j < pointsPerSensor; j++ {
			line := fmt.Sprintf("sensor,key=sensor%d value=%f %d", i, float64(j)*1.5, 1800000000+int64(j))
			influxWrite(line)
		}
	}
	influxWriteElapsed := time.Since(influxWriteStart)

	// === READ: single query (last 100), like old benchmark ===
	fmt.Println("Read (1 Flux query, last 100)...")
	influxReadStart := time.Now()
	flux := `from(bucket:"bench") |> range(start: 0) |> filter(fn: (r) => r._measurement == "sensor" and r.key == "sensor0") |> sort(columns: ["_time"], desc: true) |> limit(n:100)`
	influxQuery(flux)
	influxReadElapsed := time.Since(influxReadStart)

	// === MULTI-WRITE: 10 goroutines parallel HTTP ===
	fmt.Println("Multi-Write (10 goroutines, parallel HTTP)...")
	var influxMultiSuccess atomic.Uint64
	influxMultiStart := time.Now()
	wg.Add(sensors)
	for i := 0; i < sensors; i++ {
		go func(sid int) {
			defer wg.Done()
			for j := 0; j < pointsPerSensor; j++ {
				ts := 1900000000 + int64(sid*pointsPerSensor+j)
				val := float64(sid*pointsPerSensor + j)
				influxWrite(fmt.Sprintf("sensor,key=sensor%d value=%f %d", sid, val, ts))
				influxMultiSuccess.Add(1)
			}
		}(i)
	}
	wg.Wait()
	influxMultiElapsed := time.Since(influxMultiStart)

	// ============================================================
	// RESULTS
	// ============================================================
	fmt.Println("\n========================================")
	fmt.Println("=== RESULTS (GTSDB TCP, InfluxDB HTTP) ===")
	fmt.Println("========================================")
	fmt.Printf("%-15s %14s %14s %14s\n", "", "Write (10k)", "Read (1 qry)", "Multi (10k par)")
	fmt.Printf("%-15s %11.2f ms %11.2f ms %11.2f ms\n", "GTSDB",
		float64(gtsdbWrite.Microseconds())/1000.0,
		float64(gtsdbRead.Microseconds())/1000.0,
		float64(gtsdbMulti.Microseconds())/1000.0)
	fmt.Printf("%-15s %11.2f ms %11.2f ms %11.2f ms\n", "InfluxDB 2.9.1",
		float64(influxWriteElapsed.Microseconds())/1000.0,
		float64(influxReadElapsed.Microseconds())/1000.0,
		float64(influxMultiElapsed.Microseconds())/1000.0)
	fmt.Printf("\n%-15s %11.1fx %11.1fx %11.1fx\n", "GTSDB faster",
		float64(influxWriteElapsed)/float64(gtsdbWrite),
		float64(influxReadElapsed)/float64(gtsdbRead),
		float64(influxMultiElapsed)/float64(gtsdbMulti))

	fmt.Println("\n=== PAGE DATA ===")
	fmt.Printf("writeData:      GTSDB %.2f / InfluxDB %.2f\n",
		float64(gtsdbWrite.Microseconds())/1000.0,
		float64(influxWriteElapsed.Microseconds())/1000.0)
	fmt.Printf("readData:       GTSDB %.2f / InfluxDB %.2f\n",
		float64(gtsdbRead.Microseconds())/1000.0,
		float64(influxReadElapsed.Microseconds())/1000.0)
	fmt.Printf("multiWriteData: GTSDB %.2f / InfluxDB %.2f\n",
		float64(gtsdbMulti.Microseconds())/1000.0,
		float64(influxMultiElapsed.Microseconds())/1000.0)
	fmt.Printf("\nRead: 1 query (last 100 records). Multi-write: %d goroutines parallel.\n", sensors)
}
