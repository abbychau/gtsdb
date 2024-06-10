package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	models "gtsdb/models"
)

func TestMain(t *testing.T) {
	// Delete the data files (sensor1.aof, sensor2.aof, sensor1.idx, sensor2.idx, sensor1.meta, sensor2.meta) before running the test
	files := []string{"sensor1.aof", "sensor2.aof", "sensor1.idx", "sensor2.idx", "sensor1.meta", "sensor2.meta"}
	for _, file := range files {
		os.Remove(file)
	}

	// Start the server
	go main()

	// Wait for the server to start
	time.Sleep(3 * time.Second)

	// Connect to the server
	conn, err := net.Dial("tcp", ":5555")
	if err != nil {
		t.Fatal("Error connecting to server:", err)
	}
	defer conn.Close()

	// Test storing data points
	startTimestamp := time.Now().Unix()

	for i := 0; i < 80000; i++ {
		timestamp := startTimestamp + int64(i)
		value1 := rand.Float64() * 100
		value2 := rand.Float64() * 100

		fmt.Fprintf(conn, "sensor1,%d,%.2f\n", timestamp, value1)
		response, _ := bufio.NewReader(conn).ReadString('\n')
		if strings.TrimSpace(response) != "Data point stored" {
			t.Error("Unexpected response when storing data point:", response)
		}

		fmt.Fprintf(conn, "sensor2,%d,%.2f\n", timestamp, value2)
		response, _ = bufio.NewReader(conn).ReadString('\n')
		if strings.TrimSpace(response) != "Data point stored" {
			t.Error("Unexpected response when storing data point:", response)
		}
	}

}

// go test -v -run TestMainRead
func TestMainRead(t *testing.T) {
	// Start the server
	go main()

	// Wait for the server to start
	time.Sleep(3 * time.Second)

	// Connect to the server
	conn, err := net.Dial("tcp", ":5555")
	if err != nil {
		t.Fatal("Error connecting to server:", err)
	}
	defer conn.Close()

	// Test reading data points
	fmt.Fprintf(conn, "sensor1,%d,%d,%d\n", 0, 1717976639, 1000)
	response, _ := bufio.NewReader(conn).ReadString('\n')
	// assert len > 0
	if len(response) == 0 {
		t.Error("Unexpected response when reading data points:", response)
	}
}

func TestDownsampleDataPoints(t *testing.T) {
	dataPoints := []models.DataPoint{
		{Timestamp: 1, Value: 1},
		{Timestamp: 2, Value: 2},
		{Timestamp: 3, Value: 3},
		{Timestamp: 4, Value: 4},
		{Timestamp: 5, Value: 5},
		{Timestamp: 6, Value: 6},
		{Timestamp: 7, Value: 7},
		{Timestamp: 8, Value: 8},
		{Timestamp: 9, Value: 9},
		{Timestamp: 10, Value: 10},
	}

	downsampledDataPoints := downsampleDataPoints(dataPoints, 5)
	if len(downsampledDataPoints) != 2 {
		t.Error("Unexpected number of downsampled data points:", len(downsampledDataPoints))
	}

	if downsampledDataPoints[0].Timestamp != 1 || downsampledDataPoints[0].Value != 3 {
		t.Error("Unexpected downsampled data point:", downsampledDataPoints[0])
	}

	if downsampledDataPoints[1].Timestamp != 6 || downsampledDataPoints[1].Value != 8 {
		t.Error("Unexpected downsampled data point:", downsampledDataPoints[1])
	}
}

// formatDataPoints
func TestFormatDataPoints(t *testing.T) {
	dataPoints := []models.DataPoint{
		{ID: "test", Timestamp: 1, Value: 1},
		{ID: "test", Timestamp: 2, Value: 2},
		{ID: "test", Timestamp: 3, Value: 3},
		{ID: "test", Timestamp: 4, Value: 4},
		{ID: "test", Timestamp: 5, Value: 5},
		{ID: "test", Timestamp: 6, Value: 6},
		{ID: "test", Timestamp: 7, Value: 7},
		{ID: "test", Timestamp: 8, Value: 8},
		{ID: "test", Timestamp: 9, Value: 9},
		{ID: "test", Timestamp: 10, Value: 10},
	}

	formattedDataPoints := formatDataPoints(dataPoints)
	if formattedDataPoints != "test,1,1.00|test,2,2.00|test,3,3.00|test,4,4.00|test,5,5.00|test,6,6.00|test,7,7.00|test,8,8.00|test,9,9.00|test,10,10.00\n" {
		t.Error("Unexpected formatted data points:", formattedDataPoints)
	}
}

// benchmark
// go test -run=nonthingplease -bench BenchmarkMain -benchtime 10s
func BenchmarkMain(b *testing.B) {
	// os.RemoveAll("data")
	// os.Mkdir("data", 0755)

	// Connect to the server
	conn, err := net.Dial("tcp", ":5555")
	if err != nil {
		b.Fatal("Error connecting to server:", err)
	}
	defer conn.Close()

	// Test storing data points
	startTimestamp := time.Now().Unix()

	for i := 0; i < b.N; i++ {
		timestamp := startTimestamp + int64(i)
		value1 := rand.Float64() * 100

		name := fmt.Sprintf("sensor%d", (i*97)%100)

		// randomly select read or write
		if i%2 == 0 {
			fmt.Fprintf(conn, "%s,%d,%.2f\n", name, timestamp, value1) // write
			response, _ := bufio.NewReader(conn).ReadString('\n')
			if response != "Data point stored\n" {
				b.Error("Unexpected response when storing data point:", response)
			}

		} else {
			offset := rand.Int63n(100)
			offset = min(offset, int64(i))
			fmt.Fprintf(conn, "%s,%d,%d,%d\n", name, startTimestamp+offset, startTimestamp+offset+1000, 1) // read
			response, _ := bufio.NewReader(conn).ReadString('\n')
			if len(response) == 0 {
				b.Error("Unexpected response when reading data points:", response)
			}
		}

	}
}
