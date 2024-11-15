package main

import (
	"encoding/json"
	"fmt"
	"gtsdb/buffer"
	"gtsdb/models"
	"net/http"
	"time"
)

type WriteRequest struct {
	ID        string  `json:"id"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp,omitempty"`
}

type ReadRequest struct {
	ID          string `json:"id"`
	StartTime   int64  `json:"startTime,omitempty"`
	EndTime     int64  `json:"endTime,omitempty"`
	Downsample  int    `json:"downsample,omitempty"`
	LastX       int    `json:"lastx,omitempty"`
	Aggregation string `json:"aggregation,omitempty"`
}

type Operation struct {
	Operation string        `json:"operation"` // "write", "read", "flush", "subscribe"
	Write     *WriteRequest `json:"write,omitempty"`
	Read      *ReadRequest  `json:"read,omitempty"`
	DeviceID  string        `json:"deviceId,omitempty"`
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func writeJSON(w http.ResponseWriter, response Response) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func setupHTTPRoutes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, Response{Success: false, Message: "Method not allowed"})
			return
		}

		var op Operation
		if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
			writeJSON(w, Response{Success: false, Message: "Invalid request body"})
			return
		}

		switch op.Operation {
		case "write":
			if op.Write == nil {
				writeJSON(w, Response{Success: false, Message: "Write data required"})
				return
			}
			if op.Write.Timestamp <= 0 {
				op.Write.Timestamp = time.Now().Unix()
			}
			dataPoint := models.DataPoint{
				ID:        op.Write.ID,
				Timestamp: op.Write.Timestamp,
				Value:     op.Write.Value,
			}
			buffer.StoreDataPointBuffer(dataPoint)
			writeJSON(w, Response{Success: true, Message: "Data point stored"})

		case "read":
			if op.Read == nil {
				writeJSON(w, Response{Success: false, Message: "Read parameters required"})
				return
			}
			if op.Read.Aggregation == "" {
				op.Read.Aggregation = "avg"
			}

			var response []models.DataPoint
			if op.Read.LastX > 0 || (op.Read.StartTime == 0 && op.Read.EndTime == 0) {
				last := op.Read.LastX
				if last == 0 {
					last = 1
				}
				lastDataPoints := buffer.ReadLastDataPoints(op.Read.ID, last)
				response = lastDataPoints
			} else {
				dataPoints := buffer.ReadDataPoints(op.Read.ID, op.Read.StartTime, op.Read.EndTime, op.Read.Downsample, op.Read.Aggregation)
				response = dataPoints
			}
			writeJSON(w, Response{Success: true, Data: response})

		case "flush":
			buffer.FlushRemainingDataPoints()
			writeJSON(w, Response{Success: true, Message: "Data points flushed"})

		case "subscribe":
			if op.DeviceID == "" {
				writeJSON(w, Response{Success: false, Message: "Device ID required"})
				return
			}
			handleSSE(w, op.DeviceID)

		default:
			writeJSON(w, Response{Success: false, Message: "Invalid operation"})
		}
	})

	return mux
}

func handleSSE(w http.ResponseWriter, deviceID string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	id := time.Now().UnixNano()
	fanoutManager.AddConsumer(int(id), func(msg models.DataPoint) {
		if msg.ID == deviceID {
			resp := Response{Success: true, Data: msg}
			jsonData, _ := json.Marshal(resp)
			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			flusher.Flush()
		}
	})

	// Wait until connection is closed
	select {}
}
