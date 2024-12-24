package handlers

import (
	"gtsdb/buffer"
	"gtsdb/models"
	"gtsdb/utils"
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
	Operation string        `json:"operation"` // "write", "read", "flush", "subscribe", "unsubscribe", "initkey", "renamekey", "deletekey"
	Write     *WriteRequest `json:"write,omitempty"`
	Read      *ReadRequest  `json:"read,omitempty"`
	Key       string        `json:"key,omitempty"`
	ToKey     string        `json:"tokey,omitempty"`
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func HandleOperation(op Operation) Response {
	loweredOperation := strings.ToLower(op.Operation)

	if loweredOperation != "serverinfo" && loweredOperation != "ids" && loweredOperation != "flush" && op.Key == "" {
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
		return Response{Success: true, Message: "Key initialized"}
	case "renamekey":
		buffer.RenameKey(op.Key, op.ToKey)
		return Response{Success: true, Message: "Key renamed"}

	case "deletekey":
		buffer.DeleteKey(op.Key)
		return Response{Success: true, Message: "Key deleted"}
	case "write":
		if op.Write == nil {
			return Response{Success: false, Message: "Write data required"}
		}
		if op.Write.Timestamp <= 0 {
			op.Write.Timestamp = time.Now().Unix()
		}

		dataPoint := models.DataPoint{
			ID:        op.Key,
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
		utils.Log("Read request: %v", op.Read)
		var response []models.DataPoint
		if op.Read.LastX > 0 || (op.Read.StartTime == 0 && op.Read.EndTime == 0) {
			last := op.Read.LastX
			if last == 0 {
				last = 1
			}
			if last < 0 {
				last = last * -1
			}
			response = buffer.ReadLastDataPoints(op.Key, last)
		} else {
			response = buffer.ReadDataPoints(op.Key, op.Read.StartTime, op.Read.EndTime, op.Read.Downsample, op.Read.Aggregation)
		}
		return Response{Success: true, Data: response}
	case "ids":
		return Response{Success: true, Data: buffer.GetAllIds()}
	case "idswithcount":
		return Response{Success: true, Data: buffer.GetAllIdsWithCount()}
	case "flush":
		buffer.FlushRemainingDataPoints()
		return Response{Success: true, Message: "Data points flushed"}

	default:
		return Response{Success: false, Message: "Invalid operation"}
	}
}
