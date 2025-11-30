package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"gtsdb/auth"
	"gtsdb/fanout"
	"gtsdb/models"
	"gtsdb/utils"
	"net/http"
	"strings"
	"time"
)

func writeJSON(w http.ResponseWriter, response Response) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func authenticateRequest(r *http.Request) (auth.User, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		if utils.NoAuthUser != "" {
			if u, ok := auth.GetUser(utils.NoAuthUser); ok {
				return u, nil
			}
		}
		return auth.User{}, errors.New("unauthorized")
	}

	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		return auth.User{}, errors.New("invalid auth header")
	}

	user, ok := auth.VerifyToken(parts[1])
	if !ok {
		return auth.User{}, errors.New("invalid token")
	}
	return user, nil
}

func SetupHTTPRoutes(fanoutManager *fanout.Fanout) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		user, err := authenticateRequest(r)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		if r.Method != http.MethodPost {
			writeJSON(w, Response{Success: false, Message: "Method not allowed"})
			return
		}

		var op Operation
		if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
			writeJSON(w, Response{Success: false, Message: "Invalid request body"})
			return
		}

		if op.Operation == "adduser" {
			if user.Name != "root" {
				writeJSON(w, Response{Success: false, Message: "Unauthorized"})
				return
			}
			newUser, err := auth.CreateUser(op.Key)
			if err != nil {
				writeJSON(w, Response{Success: false, Message: err.Error()})
				return
			}
			writeJSON(w, Response{Success: true, Data: newUser})
			return
		}

		if op.Operation == "resetkey" {
			if user.Name != "root" {
				writeJSON(w, Response{Success: false, Message: "Unauthorized"})
				return
			}
			token, err := auth.ResetUserToken(op.Key)
			if err != nil {
				writeJSON(w, Response{Success: false, Message: err.Error()})
				return
			}
			writeJSON(w, Response{Success: true, Data: map[string]string{"token": token}})
			return
		}

		// Prefix keys
		prefix := user.Name + "/"
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
				writeJSON(w, Response{Success: false, Message: "Device ID required"})
				return
			}
			handleSSE(w, op.Key, fanoutManager)
			return
		}

		response := HandleOperation(op)

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
