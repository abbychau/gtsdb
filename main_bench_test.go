package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"
)

// benchmark
// go test -run=nonthingplease -bench BenchmarkMain -benchtime 10s
func BenchmarkMain(b *testing.B) {

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

		name := fmt.Sprintf("sensor%d", i%100)

		// randomly select read or write
		if i%2 == 0 {
			// {
			// 	"operation":"write",
			// 	"Write": {
			// 		"ID" : "a_sensor1",
			// 		"Value": 32242424243333333333.3333
			// 	}
			// }
			strWrite := "{\"operation\":\"write\",\"Key\":\"%s\",\"Write\":{\"Timestamp\":%d,\"Value\":%f}}\n"
			fmt.Fprintf(conn, strWrite, name, timestamp, value1) // write
			response, _ := bufio.NewReader(conn).ReadString('\n')
			if response != "{\"success\":true,\"message\":\"Data point stored\"}\n" {
				b.Error("Unexpected response when storing data point:", response)
			}

		} else {

			// random 1/2 chance to read from the last
			dice := rand.Intn(2)

			strRead := "{\"operation\":\"read\",\"Read\":{\"Key\":\"%s\",\"startTime\":%d,\"endTime\":%d,\"aggregation\":%d}}\n"

			if dice == 0 {
				offset := rand.Int63n(100)
				offset = min(offset, int64(i))

				fmt.Fprintf(conn, strRead, name, startTimestamp+offset, startTimestamp+offset+1000, 1) // random read
			} else {
				fmt.Fprintf(conn, strRead, name, 0, 0, -1) // read from the last
			}
			response, _ := bufio.NewReader(conn).ReadString('\n')
			if len(response) == 0 {
				b.Error("Unexpected response when reading data points:", response)
			}
		}
	}

}

// benchmark by simulating pub-sub queue usage
// go test -run=nonthingplease -bench BenchmarkMainPubSub -benchtime 10s

func BenchmarkMainPubSub(b *testing.B) {
	const numSubscribers = 10
	subscribers := make([]net.Conn, numSubscribers)
	publisher, err := net.Dial("tcp", ":5555")

	if err != nil {
		b.Fatal("Error connecting to server:", err)
	}

	defer publisher.Close()
	defer func() {
		for _, conn := range subscribers {
			conn.Close()
		}
	}()

}
