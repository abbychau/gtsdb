package main

import (
	"bufio"
	"fmt"
	models "gtsdb/models"
	"math/rand"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	id := rand.Intn(1000) + int(time.Now().UnixNano())
	scanner := bufio.NewScanner(conn)
	subscribingDevices := []string{}
	for scanner.Scan() {
		message := scanner.Text()
		parts := strings.Split(message, ",")

		action := parts[0]
		switch action {
		case "subscribe":
			subscribingDevices = append(subscribingDevices, parts[1])
			if len(subscribingDevices) == 1 { // First subscription
				fmt.Printf("Adding consumer %d %v\n", id, subscribingDevices)
				fanoutManager.AddConsumer(id, func(msg models.DataPoint) {
					fmt.Println("Received message:", msg)
					if slices.Contains(subscribingDevices, msg.ID) {
						conn.Write([]byte(fmt.Sprintf("%v\n", msg)))
					}
				})
			}
			conn.Write([]byte("msg:Subscribed to " + parts[1] + "\n"))

		case "unsubscribe":
			for i, device := range subscribingDevices {
				if device == parts[1] {
					subscribingDevices = append(subscribingDevices[:i], subscribingDevices[i+1:]...)
					break
				}
			}
			if len(subscribingDevices) == 0 {
				fmt.Printf("Removing consumer %d\n", id)
				fanoutManager.RemoveConsumer(id)
			}
			conn.Write([]byte("msg:Unsubscribed from " + parts[1] + "\n"))

		case "write":
			if len(parts) != 4 {
				conn.Write([]byte("Invalid write format\n"))
				continue
			}
			id := parts[1]
			timestamp, tsErr := strconv.ParseInt(parts[2], 10, 64)
			if tsErr != nil || timestamp <= 0 {
				timestamp = time.Now().Unix()
			}

			value, _ := strconv.ParseFloat(parts[3], 64)

			dataPoint := models.DataPoint{
				ID:        id,
				Timestamp: timestamp,
				Value:     value,
			}
			storeDataPointBuffer(dataPoint)
			conn.Write([]byte("Data point stored\n"))

		case "read":
			if len(parts) < 5 {
				conn.Write([]byte("Invalid read format\n"))
				continue
			}
			id := parts[1]
			startTime, _ := strconv.ParseInt(parts[2], 10, 64)
			endTime, _ := strconv.ParseInt(parts[3], 10, 64)
			downsample, _ := strconv.Atoi(parts[4])

			var response string
			if downsample < 0 {
				lastDataPoints, err := readLastDataPoints(id, downsample*-1)
				if err != nil {
					response = "Error reading last data point\n"
				} else {
					response = formatDataPoints(lastDataPoints)
				}
			} else {
				aggregation := "avg"
				if len(parts) == 6 {
					aggregation = parts[5]
				}
				dataPoints := readDataPoints(id, startTime, endTime, downsample, aggregation)
				response = formatDataPoints(dataPoints)
			}
			conn.Write([]byte(response))
		case "flush":
			flushRemainingDataPoints()
			conn.Write([]byte("Data points flushed\n"))

		default:
			conn.Write([]byte("Invalid action\n"))
		}
	}
}
