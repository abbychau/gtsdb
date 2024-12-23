package handlers

import (
	"gtsdb/buffer"
	"gtsdb/models"
	"gtsdb/utils"
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
	Operation  string        `json:"operation"` // "write", "read", "flush", "subscribe", "unsubscribe", "initkey", "renamekey", "deletekey"
	Write      *WriteRequest `json:"write,omitempty"`
	Read       *ReadRequest  `json:"read,omitempty"`
	DeviceID   string        `json:"deviceId,omitempty"`
	ToDeviceID string        `json:"toDeviceId,omitempty"`
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func HandleOperation(op Operation) Response {
	switch op.Operation {
	case "serverInfo":
		/*
			data:
			    version: "1.0"
				key-count: 332
		*/
		var data = map[string]interface{}{}
		data["version"] = "1.0"
		data["key-count"] = len(buffer.GetAllIds())
		data["health"] = "ok"

		return Response{Success: true, Data: data}
	case "initkey":
		buffer.InitKey(op.DeviceID)
		return Response{Success: true, Message: "Key initialized"}
	case "renamekey":
		if op.DeviceID == "" || op.DeviceID == "default" {
			return Response{Success: false, Message: "Invalid key name"}
		}
		buffer.RenameKey(op.DeviceID, op.ToDeviceID)
		return Response{Success: true, Message: "Key renamed"}

	case "deletekey":
		buffer.DeleteKey(op.DeviceID)
		return Response{Success: true, Message: "Key deleted"}
	case "write":
		if op.Write == nil {
			return Response{Success: false, Message: "Write data required"}
		}
		if op.Write.Timestamp <= 0 {
			op.Write.Timestamp = time.Now().Unix()
		}

		if op.Write.ID == "" {
			utils.Warning("ID required")
			return Response{Success: false, Message: "ID required"}
		}

		dataPoint := models.DataPoint{
			ID:        op.Write.ID,
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

		var response []models.DataPoint
		if op.Read.LastX > 0 || (op.Read.StartTime == 0 && op.Read.EndTime == 0) {
			last := op.Read.LastX
			if last == 0 {
				last = 1
			}
			if last < 0 {
				last = last * -1
			}
			response = buffer.ReadLastDataPoints(op.Read.ID, last)
		} else {
			response = buffer.ReadDataPoints(op.Read.ID, op.Read.StartTime, op.Read.EndTime, op.Read.Downsample, op.Read.Aggregation)
		}
		return Response{Success: true, Data: response}
	case "ids":
		return Response{Success: true, Data: buffer.GetAllIds()}
	case "flush":
		buffer.FlushRemainingDataPoints()
		return Response{Success: true, Message: "Data points flushed"}

	default:
		return Response{Success: false, Message: "Invalid operation"}
	}
}
