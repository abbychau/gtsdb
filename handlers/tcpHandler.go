package handlers

import (
	"bufio"
	"encoding/json"
	"gtsdb/buffer"
	"gtsdb/fanout"
	"gtsdb/models"
	"gtsdb/utils"

	"math/rand"
	"net"
	"slices"
	"time"
)

func HandleTcpConnection(conn net.Conn, fanoutManager *fanout.Fanout) {
	defer conn.Close()
	id := rand.Intn(1000) + int(time.Now().UnixNano())
	scanner := bufio.NewScanner(conn)
	subscribingDevices := []string{}

	// Add done channel for cleanup
	done := make(chan bool)

	// Start ping sender
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := json.NewEncoder(conn).Encode(Response{Success: true, Message: "ping"}); err != nil {
					utils.Log("Client %d failed ping", id)
					if len(subscribingDevices) > 0 {
						fanoutManager.RemoveConsumer(id)
					}
					conn.Close()
					return
				}
			}
		}
	}()

	for scanner.Scan() {
		var op Operation
		if err := json.Unmarshal(scanner.Bytes(), &op); err != nil {
			response := Response{Success: false, Message: "Invalid JSON format: " + scanner.Text()}
			json.NewEncoder(conn).Encode(response)
			continue
		}

		if op.Operation == "subscribe" {
			if op.Key == "" {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: "Device ID required"})
				continue
			}

			// If since is provided, send historical data first
			if op.Since > 0 {
				historicalData := buffer.ReadDataPoints(op.Key, op.Since, time.Now().Unix(), 0, "")
				for _, point := range historicalData {
					json.NewEncoder(conn).Encode(Response{Success: true, Data: point})
				}
			}

			subscribingDevices = append(subscribingDevices, op.Key)
			if len(subscribingDevices) == 1 {
				utils.Log("Adding consumer %d %v", id, subscribingDevices)
				fanoutManager.AddConsumer(id, func(msg models.DataPoint) {
					if slices.Contains(subscribingDevices, msg.Key) {
						json.NewEncoder(conn).Encode(Response{Success: true, Data: msg})
					}
				})
			}
			json.NewEncoder(conn).Encode(Response{Success: true, Message: "Subscribed to " + op.Key})
			continue
		}

		if op.Operation == "unsubscribe" {
			if op.Key == "" {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: "Device ID required"})
				continue
			}
			for i, device := range subscribingDevices {
				if device == op.Key {
					subscribingDevices = append(subscribingDevices[:i], subscribingDevices[i+1:]...)
					break
				}
			}
			if len(subscribingDevices) == 0 {
				utils.Log("Removing consumer %d", id)
				fanoutManager.RemoveConsumer(id)
			}
			json.NewEncoder(conn).Encode(Response{Success: true, Message: "Unsubscribed from " + op.Key})
			continue
		}

		response := HandleOperation(op)

		// if operation is write, broadcast to all consumers
		if op.Operation == "write" && response.Success {
			fanoutManager.Publish(models.DataPoint{
				Key:   op.Key,
				Value: op.Write.Value,
			})
		}

		json.NewEncoder(conn).Encode(response)
	}

	// Cleanup when the connection ends
	close(done)
	if len(subscribingDevices) > 0 {
		utils.Log("Removing consumer %d due to disconnect", id)
		fanoutManager.RemoveConsumer(id)
	}
}
