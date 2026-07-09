//go:build ignore
// +build ignore

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"
	"time"
)

func main() {
	const totalPoints = 10000
	const sensors = 10
	const pointsPerSensor = totalPoints / sensors

	conn, err := net.Dial("tcp", ":5555")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	send := func(cmd string) string {
		fmt.Fprint(conn, cmd+"\n")
		resp, _ := reader.ReadString('\n')
		return strings.TrimSpace(resp)
	}

	// Ensure keys exist
	for i := 0; i < sensors; i++ {
		send(fmt.Sprintf(`{"operation":"initkey","key":"bench_sensor_%d"}`, i))
	}
	time.Sleep(50 * time.Millisecond)

	// Pre-load data via batch-write
	fmt.Println("Pre-loading 10,000 points...")
	for i := 0; i < sensors; i++ {
		var buf bytes.Buffer
		buf.WriteString(`{"operation":"batch-write","points":[`)
		for j := 0; j < pointsPerSensor; j++ {
			if j > 0 {
				buf.WriteByte(',')
			}
			ts := 1700000000 + int64(j)
			fmt.Fprintf(&buf, `{"key":"bench_sensor_%d","value":%f,"timestamp":%d}`, i, float64(j)*1.5, ts)
		}
		buf.WriteString(`]}`)
		send(buf.String())
	}
	fmt.Println("Pre-load done.")

	// === WRITE BENCHMARK ===
	fmt.Println("\n=== WRITE (10,000 single writes, TCP reuse) ===")
	start := time.Now()
	for i := 0; i < sensors; i++ {
		for j := 0; j < pointsPerSensor; j++ {
			ts := 1800000000 + int64(j)
			cmd := fmt.Sprintf(`{"operation":"write","key":"bench_sensor_%d","write":{"timestamp":%d,"value":%f}}`, i, ts, float64(j)*1.5)
			send(cmd)
		}
	}
	writeElapsed := time.Since(start)
	fmt.Printf("Write: %v total, %.2f µs/op\n", writeElapsed, float64(writeElapsed.Microseconds())/float64(totalPoints))

	// === READ BENCHMARK ===
	fmt.Println("\n=== READ (10,000 reads, TCP reuse) ===")
	start = time.Now()
	for i := 0; i < sensors; i++ {
		for j := 0; j < pointsPerSensor; j++ {
			cmd := fmt.Sprintf(`{"operation":"read","key":"bench_sensor_%d","read":{"lastx":1}}`, i)
			send(cmd)
		}
	}
	readElapsed := time.Since(start)
	fmt.Printf("Read (10k): %v total, %.2f µs/op\n", readElapsed, float64(readElapsed.Microseconds())/float64(totalPoints))

	// === MULTI-WRITE BENCHMARK ===
	fmt.Println("\n=== MULTI-WRITE (100 batches x 100 pts = 10,000) ===")
	start = time.Now()
	var buf bytes.Buffer
	for b := 0; b < 100; b++ {
		buf.Reset()
		buf.WriteString(`{"operation":"batch-write","points":[`)
		for p := 0; p < 100; p++ {
			if p > 0 {
				buf.WriteByte(',')
			}
			sensorIdx := p % sensors
			ts := 1900000000 + int64(b*100+p)
			fmt.Fprintf(&buf, `{"key":"bench_sensor_%d","value":%f,"timestamp":%d}`, sensorIdx, float64(p)*2.5, ts)
		}
		buf.WriteString(`]}`)
		send(buf.String())
	}
	multiElapsed := time.Since(start)
	fmt.Printf("Multi-write: %v total, %.2f µs/batch\n", multiElapsed, float64(multiElapsed.Microseconds())/100.0)

	// === SUMMARY ===
	fmt.Println("\n=== PAGE DATA ===")
	fmt.Printf("writeData:      { db: \"GTSDB\", milliseconds: %.2f }\n", float64(writeElapsed.Microseconds())/1000.0)
	fmt.Printf("readData:       { db: \"GTSDB\", milliseconds: %.2f }\n", float64(readElapsed.Microseconds())/1000.0)
	fmt.Printf("multiWriteData: { db: \"GTSDB\", milliseconds: %.2f }\n", float64(multiElapsed.Microseconds())/1000.0)

	// Cleanup
	for i := 0; i < sensors; i++ {
		send(fmt.Sprintf(`{"operation":"deletekey","key":"bench_sensor_%d"}`, i))
	}
}
