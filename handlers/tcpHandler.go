package handlers

import (
	"bufio"
	"encoding/json"
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

	for scanner.Scan() {
		var op Operation
		if err := json.Unmarshal(scanner.Bytes(), &op); err != nil {
			response := Response{Success: false, Message: "Invalid JSON format: " + scanner.Text()}
			json.NewEncoder(conn).Encode(response)
			continue
		}

		if op.Operation == "subscribe" {
			if op.DeviceID == "" {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: "Device ID required"})
				continue
			}
			subscribingDevices = append(subscribingDevices, op.DeviceID)
			if len(subscribingDevices) == 1 {
				utils.Log("Adding consumer %d %v", id, subscribingDevices)
				fanoutManager.AddConsumer(id, func(msg models.DataPoint) {
					if slices.Contains(subscribingDevices, msg.ID) {
						json.NewEncoder(conn).Encode(Response{Success: true, Data: msg})
					}
				})
			}
			json.NewEncoder(conn).Encode(Response{Success: true, Message: "Subscribed to " + op.DeviceID})
			continue
		}

		if op.Operation == "unsubscribe" {
			if op.DeviceID == "" {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: "Device ID required"})
				continue
			}
			for i, device := range subscribingDevices {
				if device == op.DeviceID {
					subscribingDevices = append(subscribingDevices[:i], subscribingDevices[i+1:]...)
					break
				}
			}
			if len(subscribingDevices) == 0 {
				utils.Log("Removing consumer %d", id)
				fanoutManager.RemoveConsumer(id)
			}
			json.NewEncoder(conn).Encode(Response{Success: true, Message: "Unsubscribed from " + op.DeviceID})
			continue
		}

		response := HandleOperation(op)
		json.NewEncoder(conn).Encode(response)
	}
}
