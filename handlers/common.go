package handlers

import (
	"fmt"
	"gtsdb/buffer"
	"gtsdb/models"
	"gtsdb/utils"
	"strconv"
	"strings"
	"time"
)

type WriteRequest struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp,omitempty"`
}

type ReadRequest struct {
	StartTime   int64  `json:"start_timestamp,omitempty"`
	EndTime     int64  `json:"end_timestamp,omitempty"`
	Downsample  int    `json:"downsampling,omitempty"`
	LastX       int    `json:"lastx,omitempty"`
	Aggregation string `json:"aggregation,omitempty"`
}

type Operation struct {
	Operation string        `json:"operation"` // "write", "read", "flush", "subscribe", "unsubscribe", "initkey", "renamekey", "deletekey", "multi-read", "data-patch"
	Write     *WriteRequest `json:"write,omitempty"`
	Read      *ReadRequest  `json:"read,omitempty"`
	Key       string        `json:"key,omitempty"`
	ToKey     string        `json:"tokey,omitempty"`
	Keys      []string      `json:"keys,omitempty"`
	Data      string        `json:"data,omitempty"`  // CSV data for patch operation
	Since     int64         `json:"since,omitempty"` // Optional timestamp for subscribe operation
}

type Response struct {
	Success         bool                          `json:"success"`
	Message         string                        `json:"message,omitempty"`
	Data            interface{}                   `json:"data,omitempty"`
	ReadQueryParams *ReadRequest                  `json:"read_query_params,omitempty"`
	MultiData       map[string][]models.DataPoint `json:"multi_data,omitempty"`
}

// actions that no need key
var noKeyActions = map[string]bool{
	"serverinfo":   true,
	"ids":          true,
	"flush":        true,
	"idswithcount": true,
	"multi-read":   true,
}

func HandleOperation(op Operation) Response {
	loweredOperation := strings.ToLower(op.Operation)

	if !noKeyActions[loweredOperation] && op.Key == "" {
		return Response{Success: false, Message: "Key required"}
	}

	switch loweredOperation {
	case "serverinfo":
		var data = map[string]interface{}{}
		data["version"] = "1.0"
		data["key-count"] = len(buffer.GetAllIds())
		data["health"] = "ok"

		return Response{Success: true, Data: data}
	case "initkey":
		buffer.InitKey(op.Key)
		return Response{Success: true, Message: "Key initialized: " + op.Key}
	case "renamekey":
		buffer.RenameKey(op.Key, op.ToKey)
		return Response{Success: true, Message: "Key renamed: " + op.Key + " -> " + op.ToKey}

	case "deletekey":
		buffer.DeleteKey(op.Key)
		return Response{Success: true, Message: "Key deleted: " + op.Key}
	case "write":
		if op.Write == nil {
			return Response{Success: false, Message: "Write data required"}
		}
		if op.Write.Timestamp <= 0 {
			op.Write.Timestamp = time.Now().Unix()
		}

		dataPoint := models.DataPoint{
			Key:       op.Key,
			Timestamp: op.Write.Timestamp,
			Value:     op.Write.Value,
		}
		buffer.StoreDataPointBuffer(dataPoint)
		return Response{Success: true, Message: "Data point stored"}

	case "read":
		if op.Read == nil {
			return Response{Success: false, Message: "Read parameters required"}
		}
		if op.Read.Aggregation == "" {
			op.Read.Aggregation = "avg"
		}
		// start time and end time are set either both or none
		if (op.Read.StartTime == 0 && op.Read.EndTime != 0) || (op.Read.StartTime != 0 && op.Read.EndTime == 0) {
			return Response{Success: false, Message: "Both start and end time required or none"}
		}
		// start time must be less than end time
		if op.Read.StartTime > 0 && op.Read.EndTime > 0 && op.Read.StartTime > op.Read.EndTime {
			return Response{Success: false, Message: "Start time must be less than end time"}
		}
		utils.Log("Read request: %v", op.Read)
		var response []models.DataPoint
		var readQueryParams ReadRequest
		if op.Read.LastX > 0 {
			// Use lastx when explicitly specified
			last := op.Read.LastX
			if last < 0 {
				last = last * -1
			}
			readQueryParams = ReadRequest{
				LastX:       last,
				Aggregation: op.Read.Aggregation,
			}
			response = buffer.ReadLastDataPoints(op.Key, last)
		} else if op.Read.StartTime > 0 && op.Read.EndTime > 0 {
			// Use timestamp range when both start and end times are provided
			readQueryParams = ReadRequest{
				StartTime:   op.Read.StartTime,
				EndTime:     op.Read.EndTime,
				Downsample:  op.Read.Downsample,
				Aggregation: op.Read.Aggregation,
			}
			response = buffer.ReadDataPoints(op.Key, op.Read.StartTime, op.Read.EndTime, op.Read.Downsample, op.Read.Aggregation)
		} else {
			// Default to last 1 when no specific parameters are provided
			readQueryParams = ReadRequest{
				LastX:       1,
				Aggregation: op.Read.Aggregation,
			}
			response = buffer.ReadLastDataPoints(op.Key, 1)
		}
		
		// Log first record of the response
		if len(response) > 0 && response[0].Key != "" {
			utils.Log("Read response first record: Key=%s, Timestamp=%d, Value=%f", response[0].Key, response[0].Timestamp, response[0].Value)
		} else {
			utils.Log("Read response: no records found for key=%s", op.Key)
		}
		
		return Response{
			Success:         true,
			Data:            response,
			ReadQueryParams: &readQueryParams,
		}
	case "multi-read":
		if op.Read == nil {
			return Response{Success: false, Message: "Read parameters required"}
		}
		if len(op.Keys) == 0 {
			return Response{Success: false, Message: "Keys array required"}
		}
		if op.Read.Aggregation == "" {
			op.Read.Aggregation = "avg"
		}
		// start time and end time are set either both or none
		if (op.Read.StartTime == 0 && op.Read.EndTime != 0) || (op.Read.StartTime != 0 && op.Read.EndTime == 0) {
			return Response{Success: false, Message: "Both start and end time required or none"}
		}
		// start time must be less than end time
		if op.Read.StartTime > 0 && op.Read.EndTime > 0 && op.Read.StartTime > op.Read.EndTime {
			return Response{Success: false, Message: "Start time must be less than end time"}
		}

		result := make(map[string][]models.DataPoint)
		for _, key := range op.Keys {
			var response []models.DataPoint
			if op.Read.LastX > 0 {
				// Use lastx when explicitly specified
				last := op.Read.LastX
				if last < 0 {
					last = last * -1
				}
				response = buffer.ReadLastDataPoints(key, last)
			} else if op.Read.StartTime > 0 && op.Read.EndTime > 0 {
				// Use timestamp range when both start and end times are provided
				response = buffer.ReadDataPoints(key, op.Read.StartTime, op.Read.EndTime, op.Read.Downsample, op.Read.Aggregation)
			} else {
				// Default to last 1 when no specific parameters are provided
				response = buffer.ReadLastDataPoints(key, 1)
			}
			
			// Log first record of the response for each key
			if len(response) > 0 && response[0].Key != "" {
				utils.Log("Multi-read response first record: Key=%s, Timestamp=%d, Value=%f", response[0].Key, response[0].Timestamp, response[0].Value)
			} else {
				utils.Log("Multi-read response: no records found for key=%s", key)
			}
			
			result[key] = response
		}

		return Response{
			Success:         true,
			MultiData:       result,
			ReadQueryParams: op.Read,
		}
	case "ids":
		return Response{Success: true, Data: buffer.GetAllIds()}
	case "idswithcount":
		return Response{Success: true, Data: buffer.GetAllIdsWithCount()}
	case "flush":
		buffer.FlushRemainingDataPoints()
		return Response{Success: true, Message: "Data flushed"}

	case "data-patch":
		if op.Data == "" {
			return Response{Success: false, Message: "CSV data required"}
		}

		var points []models.DataPoint
		rows := strings.Split(op.Data, "\n")
		for _, row := range rows {
			row = strings.TrimSpace(row)
			if row == "" {
				continue
			}
			parts := strings.Split(row, ",")
			if len(parts) != 2 {
				continue
			}
			timestamp, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				continue
			}
			value, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				continue
			}
			points = append(points, models.DataPoint{
				Key:       op.Key,
				Timestamp: timestamp,
				Value:     value,
			})
		}

		if len(points) == 0 {
			return Response{Success: false, Message: "No valid data points found in CSV"}
		}

		buffer.PatchDataPoints(points, op.Key)

		return Response{Success: true, Message: fmt.Sprintf("Patched %d data points", len(points))}

	default:
		return Response{Success: false, Message: "Invalid operation"}
	}
}
