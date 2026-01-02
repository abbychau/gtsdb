package handlers

import (
	"bufio"
	"encoding/json"
	"gtsdb/auth"
	"gtsdb/buffer"
	"gtsdb/fanout"
	"gtsdb/models"
	"gtsdb/utils"
	"strings"

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

	var currentUser auth.User
	if utils.NoAuthUser != "" {
		if u, ok := auth.GetUser(utils.NoAuthUser); ok {
			currentUser = u
		}
	}

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

		if op.Operation == "auth" {
			if op.Key == "" {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: "Token required"})
				continue
			}
			user, ok := auth.VerifyToken(op.Key)
			if !ok {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: "Invalid token"})
				continue
			}
			currentUser = user
			json.NewEncoder(conn).Encode(Response{Success: true, Message: "Authenticated as " + user.Name})
			continue
		}

		if currentUser.Name == "" {
			json.NewEncoder(conn).Encode(Response{Success: false, Message: "Authentication required"})
			continue
		}

		if op.Operation == "adduser" {
			if currentUser.Name != "root" {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: "Unauthorized"})
				continue
			}
			newUser, err := auth.CreateUser(op.Key)
			if err != nil {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: err.Error()})
				continue
			}
			json.NewEncoder(conn).Encode(Response{Success: true, Data: newUser})
			continue
		}

		if op.Operation == "resetkey" {
			if currentUser.Name != "root" {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: "Unauthorized"})
				continue
			}
			token, err := auth.ResetUserToken(op.Key)
			if err != nil {
				json.NewEncoder(conn).Encode(Response{Success: false, Message: err.Error()})
				continue
			}
			json.NewEncoder(conn).Encode(Response{Success: true, Data: map[string]string{"token": token}})
			continue
		}

		// Prefix keys
		prefix := currentUser.Name + "/"
		if op.Key != "" {
			op.Key = prefix + op.Key
		}
		if op.ToKey != "" {
			op.ToKey = prefix + op.ToKey
		}
		if len(op.Keys) > 0 {
			for i, k := range op.Keys {
				op.Keys[i] = prefix + k
			}
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
					point.Key = strings.TrimPrefix(point.Key, prefix)
					json.NewEncoder(conn).Encode(Response{Success: true, Data: point})
				}
			}

			subscribingDevices = append(subscribingDevices, op.Key)
			if len(subscribingDevices) == 1 {
				utils.Log("Adding consumer %d %v", id, subscribingDevices)
				fanoutManager.AddConsumer(id, func(msg models.DataPoint) {
					if slices.Contains(subscribingDevices, msg.Key) {
						msg.Key = strings.TrimPrefix(msg.Key, prefix)
						json.NewEncoder(conn).Encode(Response{Success: true, Data: msg})
					}
				})
			}
			json.NewEncoder(conn).Encode(Response{Success: true, Message: "Subscribed to " + strings.TrimPrefix(op.Key, prefix)})
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
			json.NewEncoder(conn).Encode(Response{Success: true, Message: "Unsubscribed from " + strings.TrimPrefix(op.Key, prefix)})
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

		// Filter and Unprefix response
		switch op.Operation {
		case "ids":
			if ids, ok := response.Data.([]string); ok {
				var filtered []string
				for _, id := range ids {
					if strings.HasPrefix(id, prefix) {
						filtered = append(filtered, strings.TrimPrefix(id, prefix))
					}
				}
				response.Data = filtered
			}
		case "idswithcount":
			if keyCounts, ok := response.Data.([]models.KeyCount); ok {
				var filtered []models.KeyCount
				for _, kc := range keyCounts {
					if strings.HasPrefix(kc.Key, prefix) {
						kc.Key = strings.TrimPrefix(kc.Key, prefix)
						filtered = append(filtered, kc)
					}
				}
				response.Data = filtered
			}
		case "read":
			if dataPoints, ok := response.Data.([]models.DataPoint); ok {
				for i := range dataPoints {
					dataPoints[i].Key = strings.TrimPrefix(dataPoints[i].Key, prefix)
				}
				response.Data = dataPoints
			}
		case "multi-read":
			if response.MultiData != nil {
				newMultiData := make(map[string][]models.DataPoint)
				for k, v := range response.MultiData {
					if strings.HasPrefix(k, prefix) {
						newKey := strings.TrimPrefix(k, prefix)
						for i := range v {
							v[i].Key = strings.TrimPrefix(v[i].Key, prefix)
						}
						newMultiData[newKey] = v
					}
				}
				response.MultiData = newMultiData
			}
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
