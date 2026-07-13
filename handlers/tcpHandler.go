package handlers

import (
	"bufio"
	"gtsdb/auth"
	"gtsdb/buffer"
	"gtsdb/fanout"
	"gtsdb/models"
	"gtsdb/utils"
	"strings"
	"sync"

	json "github.com/velox-io/json"

	"math/rand"
	"net"
	"slices"
	"time"
)

// writeTCPResponse encodes a Response as JSON to the TCP connection.
func writeTCPResponse(conn net.Conn, resp Response) bool {
	data, err := json.Marshal(resp)
	if err != nil {
		return false
	}
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err == nil
}

func connWriteJSON(conn net.Conn, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}

func HandleTcpConnection(conn net.Conn, fanoutManager *fanout.Fanout) {
	defer conn.Close()
	id := rand.Intn(1000) + int(time.Now().UnixNano())
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB max token size for batch writes
	subscribingDevices := []string{}

	var currentUser auth.User
	if utils.NoAuthUser != "" {
		if u, ok := auth.GetUser(utils.NoAuthUser); ok {
			currentUser = u
		}
	}

	// Use sync.Once to ensure cleanup runs exactly once
	done := make(chan bool)
	var cleanupOnce sync.Once
	cleanup := func() {
		close(done)
		if len(subscribingDevices) > 0 {
			utils.Log("Removing consumer %d due to disconnect", id)
			fanoutManager.RemoveConsumer(id)
		}
	}

	// Start ping sender
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := connWriteJSON(conn, Response{Success: true, Message: "ping"}); err != nil {
					utils.Log("Client %d failed ping", id)
					cleanupOnce.Do(cleanup)
					return
				}
			}
		}
	}()

	for scanner.Scan() {
		var op Operation
		if err := json.Unmarshal(scanner.Bytes(), &op); err != nil {
			connWriteJSON(conn, Response{Success: false, Message: "Invalid JSON format: " + scanner.Text()})
			continue
		}

		if op.Operation == "auth" {
			if op.Key == "" {
				writeTCPResponse(conn, Response{Success: false, Message: "Token required"})
				continue
			}
			user, ok := auth.VerifyToken(op.Key)
			if !ok {
				writeTCPResponse(conn, Response{Success: false, Message: "Invalid token"})
				continue
			}
			currentUser = user
			writeTCPResponse(conn, Response{Success: true, Message: "Authenticated as " + user.Name})
			continue
		}

		if currentUser.Name == "" {
			writeTCPResponse(conn, Response{Success: false, Message: "Authentication required"})
			continue
		}

		if op.Operation == "adduser" {
			if currentUser.Name != "root" {
				writeTCPResponse(conn, Response{Success: false, Message: "Unauthorized"})
				continue
			}
			newUser, err := auth.CreateUser(op.Key)
			if err != nil {
				writeTCPResponse(conn, Response{Success: false, Message: err.Error()})
				continue
			}
			writeTCPResponse(conn, Response{Success: true, Data: newUser})
			continue
		}

		if op.Operation == "resetkey" {
			if currentUser.Name != "root" {
				writeTCPResponse(conn, Response{Success: false, Message: "Unauthorized"})
				continue
			}
			token, err := auth.ResetUserToken(op.Key)
			if err != nil {
				writeTCPResponse(conn, Response{Success: false, Message: err.Error()})
				continue
			}
			writeTCPResponse(conn, Response{Success: true, Data: map[string]string{"token": token}})
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
		if len(op.Points) > 0 {
			for i := range op.Points {
				op.Points[i].Key = prefix + op.Points[i].Key
			}
		}

		if op.Operation == "subscribe" {
			if op.Key == "" {
				writeTCPResponse(conn, Response{Success: false, Message: "Device ID required"})
				continue
			}

			// If since is provided, send historical data first
			if op.Since > 0 {
				historicalData := buffer.ReadDataPoints(op.Key, op.Since, time.Now().Unix(), 0, "")
				for _, point := range historicalData {
					point.Key = strings.TrimPrefix(point.Key, prefix)
					writeTCPResponse(conn, Response{Success: true, Data: point})
				}
			}

			subscribingDevices = append(subscribingDevices, op.Key)
			if len(subscribingDevices) == 1 {
				utils.Log("Adding consumer %d %v", id, subscribingDevices)
				fanoutManager.AddConsumer(id, func(msg models.DataPoint) {
					if slices.Contains(subscribingDevices, msg.Key) {
						msg.Key = strings.TrimPrefix(msg.Key, prefix)
						writeTCPResponse(conn, Response{Success: true, Data: msg})
					}
				})
			}
			writeTCPResponse(conn, Response{Success: true, Message: "Subscribed to " + strings.TrimPrefix(op.Key, prefix)})
			continue
		}

		if op.Operation == "unsubscribe" {
			if op.Key == "" {
				writeTCPResponse(conn, Response{Success: false, Message: "Device ID required"})
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
			writeTCPResponse(conn, Response{Success: true, Message: "Unsubscribed from " + strings.TrimPrefix(op.Key, prefix)})
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

		writeTCPResponse(conn, response)
	}

	// Cleanup when the connection ends (safe via sync.Once)
	if err := scanner.Err(); err != nil {
		utils.Log("Client %d scanner error: %v", id, err)
	}
	cleanupOnce.Do(cleanup)
}
