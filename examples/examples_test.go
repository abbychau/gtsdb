package examples

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(t *testing.T) {
	// Delete the data files (any file in data folder)
	files, _ := os.ReadDir("data")
	for _, file := range files {
		t.Log("Removing file:", file.Name())
		e := os.Remove("data/" + file.Name())
		if e != nil {
			t.Fatal("Error removing file:", e)
		}
	}
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

		fmt.Fprintf(conn, "write,sensor1,%d,%.2f\n", timestamp, value1)
		response, _ := bufio.NewReader(conn).ReadString('\n')
		if strings.TrimSpace(response) != "Data point stored" {
			t.Error("Unexpected response when storing data point:", response)
		}

		fmt.Fprintf(conn, "write,sensor2,%d,%.2f\n", timestamp, value2)
		response, _ = bufio.NewReader(conn).ReadString('\n')
		if strings.TrimSpace(response) != "Data point stored" {
			t.Error("Unexpected response when storing data point:", response)
		}
	}

}

// go test -v -run TestMainRead
func TestMainRead(t *testing.T) {

	// Wait for the server to start
	time.Sleep(3 * time.Second)

	// Connect to the server
	conn, err := net.Dial("tcp", ":5555")
	if err != nil {
		t.Fatal("Error connecting to server:", err)
	}
	defer conn.Close()

	// Test reading data points
	fmt.Fprintf(conn, "read,sensor1,%d,%d,%d\n", 0, 1717976639, 1000)
	response, _ := bufio.NewReader(conn).ReadString('\n')
	// assert len > 0
	if len(response) == 0 {
		t.Error("Unexpected response when reading data points:", response)
	}
}

// go test -v -run TestMainStreaming
func TestMainStreaming(t *testing.T) {
	connProducer, err := net.Dial("tcp", ":5555")
	if err != nil {
		t.Fatal("Error connecting to server:", err)
	}
	defer connProducer.Close()

	connConsumer, err := net.Dial("tcp", ":5555")
	if err != nil {
		t.Fatal("Error connecting to server:", err)
	}
	defer connConsumer.Close()

	connConsumer2, err := net.Dial("tcp", ":5555")
	if err != nil {
		t.Fatal("Error connecting to server:", err)
	}
	defer connConsumer2.Close()

	// consumer subscribe
	fmt.Fprintf(connConsumer, "subscribe,sensor1\n")
	res, _ := bufio.NewReader(connConsumer).ReadString('\n')
	if res != "msg:Subscribed to sensor1\n" {
		t.Error("Unexpected response when c1 subscribing to sensor1:", res)
	}

	// consumer2 subscribe
	fmt.Fprintf(connConsumer2, "subscribe,sensor1\n")
	res, _ = bufio.NewReader(connConsumer2).ReadString('\n')
	if res != "msg:Subscribed to sensor1\n" {
		t.Error("Unexpected response when c2 subscribing to sensor1:", res)
	}

	// producer send data
	timestamp := time.Now().Unix()
	value1 := rand.Float64() * 100
	fmt.Fprintf(connProducer, "write,sensor1,%d,%.2f\n", timestamp, value1)

	// consumer read data
	res, _ = bufio.NewReader(connConsumer).ReadString('\n')
	if res != fmt.Sprintf("sensor1,%d,%.2f\n", timestamp, value1) {
		t.Error("Unexpected response when reading data points:", res)
	}

	// consumer2 read data
	res, _ = bufio.NewReader(connConsumer2).ReadString('\n')
	if res != fmt.Sprintf("sensor1,%d,%.2f\n", timestamp, value1) {
		t.Error("Unexpected response when reading data points:", res)
	}

}

// test aggregation
func TestMainAggregation(t *testing.T) {
	// data points
	dataPoints := []string{
		"sensor1,1,1",
		"sensor1,2,2",
		"sensor1,3,3",
		"sensor1,4,4",
		"sensor1,5,5",
		"sensor1,6,6",
		"sensor1,7,7",
		"sensor1,8,8",
		"sensor1,9,9",
		"sensor1,10,10",
	}

	// Connect to the server
	conn, err := net.Dial("tcp", ":5555")
	if err != nil {
		t.Fatal("Error connecting to server:", err)
	}
	defer conn.Close()

	// expected aggregation
	expected := map[string]float64{
		"avg": 5.5,
		"sum": 55,
		"min": 1,
		"max": 10,
	}

	// store data points
	for _, dataPoint := range dataPoints {
		fmt.Fprintf(conn, "%s\n", dataPoint)
		response, _ := bufio.NewReader(conn).ReadString('\n')
		if strings.TrimSpace(response) != "Data point stored" {
			t.Error("Unexpected response when storing data point:", response)
		}
	}

	// test aggregation
	for aggregation, value := range expected {
		fmt.Fprintf(conn, "sensor1,1,1817971018,0,%s\n", aggregation)
		response, _ := bufio.NewReader(conn).ReadString('\n')
		if strings.TrimSpace(response) != fmt.Sprintf("sensor1,1,%.2f\n", value) {
			t.Error("Unexpected response when aggregating data points:", response)
		}
	}

}
