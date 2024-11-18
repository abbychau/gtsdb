package main

import (
	"bufio"
	"fmt"
	"gtsdb/utils"
	"math/rand"
	"net"
	"testing"
	"time"
)

// benchmark
// go test -run=nonthingplease -bench BenchmarkMain -benchtime 10s
func BenchmarkMain(b *testing.B) {
	tmpDir, cleanup := utils.SetupTestFiles()
	defer cleanup()

	// Set global data directory
	utils.DataDir = tmpDir

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
			strWrite := "{\"operation\":\"write\",\"Write\":{\"ID\":\"%s\",\"Timestamp\":%d,\"Value\":%f}}\n"
			fmt.Fprintf(conn, strWrite, name, timestamp, value1) // write
			response, _ := bufio.NewReader(conn).ReadString('\n')
			if response != "{\"success\":true,\"message\":\"Data point stored\"}\n" {
				b.Error("Unexpected response when storing data point:", response)
			}

		} else {

			// random 1/2 chance to read from the last
			dice := rand.Intn(2)

			/*
			   {
			       "operation":"read",
			       "Read": {
			           "ID" : "a_sensor1",
			           "startTime" : 0,
			           "endTime" : 0
			       }
			   }
			*/
			strRead := "{\"operation\":\"read\",\"Read\":{\"ID\":\"%s\",\"startTime\":%d,\"endTime\":%d,\"aggregation\":%d}}\n"

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
