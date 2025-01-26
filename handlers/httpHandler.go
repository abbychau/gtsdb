package handlers

import (
	"encoding/json"
	"fmt"
	"gtsdb/fanout"
	"gtsdb/models"
	"net/http"
	"time"
)

func writeJSON(w http.ResponseWriter, response Response) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func SetupHTTPRoutes(fanoutManager *fanout.Fanout) http.Handler {
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

		if op.Operation == "subscribe" {
			if op.Key == "" {
				writeJSON(w, Response{Success: false, Message: "Device ID required"})
				return
			}
			handleSSE(w, op.Key, fanoutManager)
			return
		}

		response := HandleOperation(op)
		writeJSON(w, response)
	})

	return mux
}

func handleSSE(w http.ResponseWriter, key string, fanoutManager *fanout.Fanout) {
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
		if msg.Key == key {
			resp := Response{Success: true, Data: msg}
			jsonData, _ := json.Marshal(resp)
			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			flusher.Flush()
		}
	})

	// Wait until connection is closed
	select {}
}
