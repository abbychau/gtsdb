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
	// Delete the data files (sensor1.aof, sensor2.aof, sensor1.idx, sensor2.idx, sensor1.meta, sensor2.meta) before running the test
	files := []string{"sensor1.aof", "sensor2.aof", "sensor1.idx", "sensor2.idx", "sensor1.meta", "sensor2.meta"}
	for _, file := range files {
		os.Remove(file)
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
	fmt.Fprintf(connProducer, "sensor1,%d,%.2f\n", timestamp, value1)

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
