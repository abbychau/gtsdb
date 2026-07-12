package handlers

import (
	"fmt"
	"gtsdb/buffer"
	"gtsdb/models"
	"gtsdb/utils"
	"runtime"
	"strconv"
	"strings"
	"time"

	json "github.com/bytedance/sonic"
)

var serverStartTime = time.Now()

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
	CountOnly   bool   `json:"count_only,omitempty"` // return only counts, not data
}

type DeleteDataPointRequest struct {
	Operator      string   `json:"operator,omitempty"`
	Value         *float64 `json:"value,omitempty"`
	TimestampFrom int64    `json:"timestampFrom,omitempty"`
	TimestampTo   int64    `json:"timestampTo,omitempty"`
}

type ExportRequest struct {
	Format      string `json:"format,omitempty"` // "csv" or "json"
	StartTime   int64  `json:"start_timestamp,omitempty"`
	EndTime     int64  `json:"end_timestamp,omitempty"`
	Downsample  int    `json:"downsampling,omitempty"`
	LastX       int    `json:"lastx,omitempty"`
	Aggregation string `json:"aggregation,omitempty"`
}

type BatchWritePoint struct {
	Key       string  `json:"key"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp,omitempty"`
}

type Operation struct {
	Operation string                  `json:"operation"` // "write", "read", "flush", "subscribe", "unsubscribe", "initkey", "renamekey", "deletekey", "reloadkey", "multi-read", "data-patch", "deleteDataPointForValue"
	Write     *WriteRequest           `json:"write,omitempty"`
	Read      *ReadRequest            `json:"read,omitempty"`
	Export    *ExportRequest          `json:"export,omitempty"`
	Payload   *DeleteDataPointRequest `json:"payload,omitempty"`
	Key       string                  `json:"key,omitempty"`
	ToKey     string                  `json:"tokey,omitempty"`
	Keys      []string                `json:"keys,omitempty"`
	Data      string                  `json:"data,omitempty"`   // CSV data for patch operation
	Points    []BatchWritePoint       `json:"points,omitempty"` // Batch write points
	Since     int64                   `json:"since,omitempty"`  // Optional timestamp for subscribe operation
}

type Response struct {
	Success         bool                          `json:"success"`
	Message         string                        `json:"message,omitempty"`
	Data            interface{}                   `json:"data,omitempty"`
	ReadQueryParams *ReadRequest                  `json:"read_query_params,omitempty"`
	MultiData       map[string][]models.DataPoint `json:"multi_data,omitempty"`
}

// MarshalJSON implements json.Marshaler with a fast path for MultiData responses.
// For multi-read, builds JSON directly without reflection.
func (r Response) MarshalJSON() ([]byte, error) {
	if r.MultiData == nil {
		// Non-multi-read: use sonic (type alias breaks recursion)
		type respAlias Response
		return json.Marshal(respAlias(r))
	}

	// Fast path for multi-read
	var sb strings.Builder
	// Estimate: ~80 bytes overhead + keys*keyLen + points*68 bytes each
	totalEst := 80 + len(r.MultiData)*32
	for _, pts := range r.MultiData {
		totalEst += len(pts) * 68
	}
	sb.Grow(totalEst)

	sb.WriteString(`{"success":true,"multi_data":{`)
	first := true
	for k, pts := range r.MultiData {
		if !first {
			sb.WriteByte(',')
		}
		first = false
		sb.WriteByte('"')
		sb.WriteString(k)
		sb.WriteString(`":[`)
		for i, dp := range pts {
			if i > 0 {
				sb.WriteByte(',')
			}
			b, _ := dp.MarshalJSON()
			sb.Write(b)
		}
		sb.WriteByte(']')
	}
	sb.WriteByte('}')

	if r.ReadQueryParams != nil {
		sb.WriteString(`,"read_query_params":`)
		qp, _ := json.Marshal(r.ReadQueryParams)
		sb.Write(qp)
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

const (
	minValidTimestamp  int64 = 946684800        // 2000-01-01
	maxValidTimestamp  int64 = 4102444800       // 2100-01-01
	maxPatchDataLength int   = 10 * 1024 * 1024 // 10MB
)

// validateTimestamp checks if a timestamp is within a reasonable range
func validateTimestamp(ts int64) bool {
	if ts > 0 && (ts < minValidTimestamp || ts > maxValidTimestamp) {
		return false
	}
	return true
}

// validateKey checks for path traversal and other unsafe characters
func validateKey(key string) bool {
	if key == "" {
		return true // empty check is handled separately
	}
	// Block path traversal
	if strings.Contains(key, "..") {
		return false
	}
	// Block null bytes
	if strings.ContainsRune(key, 0) {
		return false
	}
	// Block keys starting with / or \
	if strings.HasPrefix(key, "/") || strings.HasPrefix(key, "\\") {
		return false
	}
	// Reasonable max length
	if len(key) > 512 {
		return false
	}
	return true
}

// actions that no need key
var noKeyActions = map[string]bool{
	"serverinfo":   true,
	"ids":          true,
	"flush":        true,
	"idswithcount": true,
	"multi-read":   true,
	"batch-write":  true,
}

func HandleOperation(op Operation) Response {
	loweredOperation := strings.ToLower(op.Operation)

	if !noKeyActions[loweredOperation] && op.Key == "" {
		return Response{Success: false, Message: "Key required"}
	}

	// Validate all keys for path traversal
	if !noKeyActions[loweredOperation] && !validateKey(op.Key) {
		return Response{Success: false, Message: "Invalid key: contains unsafe characters"}
	}
	if op.ToKey != "" && !validateKey(op.ToKey) {
		return Response{Success: false, Message: "Invalid toKey: contains unsafe characters"}
	}
	for _, k := range op.Keys {
		if !validateKey(k) {
			return Response{Success: false, Message: "Invalid key in keys array: contains unsafe characters"}
		}
	}

	switch loweredOperation {
	case "serverinfo":
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		data := map[string]interface{}{
			"version":         "1.0",
			"key_count":       len(buffer.GetAllIds()),
			"health":          "ok",
			"uptime_seconds":  int(time.Since(serverStartTime).Seconds()),
			"goroutines":      runtime.NumGoroutine(),
			"memory_alloc_mb": float64(m.Alloc) / 1024 / 1024,
			"memory_total_mb": float64(m.TotalAlloc) / 1024 / 1024,
			"num_cpu":         runtime.NumCPU(),
			"listen_tcp":      utils.TcpListenAddr,
			"listen_http":     utils.HttpListenAddr,
			"data_dir":        utils.DataDir,
			"file_handle_lru": utils.FileHandleLRUCapacity,
		}
		return Response{Success: true, Data: data}
	case "export":
		if op.Export == nil {
			return Response{Success: false, Message: "Export parameters required"}
		}
		if op.Key == "" {
			return Response{Success: false, Message: "Key required"}
		}
		format := op.Export.Format
		if format == "" {
			format = "json"
		}
		if format != "csv" && format != "json" {
			return Response{Success: false, Message: "Format must be 'csv' or 'json'"}
		}

		var points []models.DataPoint
		if op.Export.LastX > 0 {
			points = buffer.ReadLastDataPoints(op.Key, op.Export.LastX)
		} else if op.Export.StartTime > 0 && op.Export.EndTime > 0 {
			points = buffer.ReadDataPoints(op.Key, op.Export.StartTime, op.Export.EndTime, op.Export.Downsample, op.Export.Aggregation)
		} else {
			points = buffer.ReadLastDataPoints(op.Key, 1000)
		}

		if format == "csv" {
			var sb strings.Builder
			sb.WriteString("key,timestamp,value\n")
			for _, p := range points {
				sb.WriteString(fmt.Sprintf("%s,%d,%f\n", p.Key, p.Timestamp, p.Value))
			}
			return Response{Success: true, Data: sb.String()}
		}
		return Response{Success: true, Data: points}
	case "initkey":
		buffer.InitKey(op.Key)
		return Response{Success: true, Message: "Key initialized: " + op.Key}
	case "renamekey":
		buffer.RenameKey(op.Key, op.ToKey)
		return Response{Success: true, Message: "Key renamed: " + op.Key + " -> " + op.ToKey}

	case "deletekey":
		buffer.DeleteKey(op.Key)
		return Response{Success: true, Message: "Key deleted: " + op.Key}
	case "reloadkey":
		ok := buffer.ReloadKey(op.Key)
		if ok {
			return Response{Success: true, Message: "Key reloaded: " + op.Key}
		}
		return Response{Success: true, Message: "Key reloaded (not found on disk): " + op.Key}
	case "write":
		if op.Write == nil {
			return Response{Success: false, Message: "Write data required"}
		}
		if op.Write.Timestamp <= 0 {
			op.Write.Timestamp = time.Now().Unix()
		} else if !validateTimestamp(op.Write.Timestamp) {
			return Response{Success: false, Message: "Timestamp out of valid range (2000-2100)"}
		}

		dataPoint := models.DataPoint{
			Key:       op.Key,
			Timestamp: op.Write.Timestamp,
			Value:     op.Write.Value,
		}
		buffer.StoreDataPointBuffer(dataPoint)
		return Response{Success: true, Message: "Data point stored"}

	case "batch-write":
		if len(op.Points) == 0 {
			return Response{Success: false, Message: "Points array required"}
		}
		if len(op.Points) > 10000 {
			return Response{Success: false, Message: "Batch size exceeds maximum (10000)"}
		}
		now := time.Now().Unix()
		dataPoints := make([]models.DataPoint, 0, len(op.Points))
		for _, p := range op.Points {
			if p.Key == "" {
				return Response{Success: false, Message: "Key required for all points"}
			}
			if !validateKey(p.Key) {
				return Response{Success: false, Message: "Invalid key in batch: " + p.Key}
			}
			ts := p.Timestamp
			if ts <= 0 {
				ts = now
			} else if !validateTimestamp(ts) {
				return Response{Success: false, Message: "Timestamp out of valid range for key: " + p.Key}
			}
			dataPoints = append(dataPoints, models.DataPoint{
				Key:       p.Key,
				Timestamp: ts,
				Value:     p.Value,
			})
		}
		buffer.StoreDataPointsBuffer(dataPoints)
		return Response{Success: true, Message: fmt.Sprintf("Stored %d data points", len(op.Points))}

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
		// validate timestamps
		if !validateTimestamp(op.Read.StartTime) || !validateTimestamp(op.Read.EndTime) {
			return Response{Success: false, Message: "Timestamp out of valid range (2000-2100)"}
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
		if (op.Read.StartTime == 0 && op.Read.EndTime != 0) || (op.Read.StartTime != 0 && op.Read.EndTime == 0) {
			return Response{Success: false, Message: "Both start and end time required or none"}
		}
		if op.Read.StartTime > 0 && op.Read.EndTime > 0 && op.Read.StartTime > op.Read.EndTime {
			return Response{Success: false, Message: "Start time must be less than end time"}
		}

		// Sequential reads: for in-memory cache hits, this is faster than goroutine overhead
		result := make(map[string][]models.DataPoint, len(op.Keys))
		for _, key := range op.Keys {
			var response []models.DataPoint
			if op.Read.LastX > 0 {
				last := op.Read.LastX
				if last < 0 {
					last = last * -1
				}
				response = buffer.ReadLastDataPoints(key, last)
			} else if op.Read.StartTime > 0 && op.Read.EndTime > 0 {
				response = buffer.ReadDataPoints(key, op.Read.StartTime, op.Read.EndTime, op.Read.Downsample, op.Read.Aggregation)
			} else {
				response = buffer.ReadLastDataPoints(key, 1)
			}
			result[key] = response
		}

		// Count-only mode: return just the count per key (tiny response)
		if op.Read.CountOnly {
			counts := make(map[string]int, len(op.Keys))
			for k, v := range result {
				counts[k] = len(v)
			}
			return Response{
				Success:         true,
				Data:            counts,
				ReadQueryParams: op.Read,
			}
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
	case "compact":
		if err := buffer.CompactKey(op.Key); err != nil {
			return Response{Success: false, Message: "Compaction failed: " + err.Error()}
		}
		return Response{Success: true, Message: "Key compacted: " + op.Key}

	case "data-patch":
		if op.Data == "" {
			return Response{Success: false, Message: "Data required (CSV or JSON array)"}
		}
		if len(op.Data) > maxPatchDataLength {
			return Response{Success: false, Message: fmt.Sprintf("Data too large: max %d bytes", maxPatchDataLength)}
		}

		var points []models.DataPoint

		// Check if data is a JSON array
		trimmedData := strings.TrimSpace(op.Data)
		if strings.HasPrefix(trimmedData, "[") {
			// Parse JSON array format: [{"timestamp": 123, "value": 45.6}, ...]
			var jsonPoints []struct {
				Timestamp int64   `json:"timestamp"`
				Value     float64 `json:"value"`
			}
			if err := json.Unmarshal([]byte(trimmedData), &jsonPoints); err != nil {
				return Response{Success: false, Message: "Invalid JSON array format: " + err.Error()}
			}
			for _, jp := range jsonPoints {
				points = append(points, models.DataPoint{
					Key:       op.Key,
					Timestamp: jp.Timestamp,
					Value:     jp.Value,
				})
			}
		} else {
			// Parse CSV format: timestamp,value per line
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
		}

		if len(points) == 0 {
			return Response{Success: false, Message: "No valid data points found in CSV or JSON"}
		}

		buffer.PatchDataPoints(points, op.Key)

		return Response{Success: true, Message: fmt.Sprintf("Patched %d data points", len(points))}
	case "deletedatapoint":
		if op.Payload == nil {
			return Response{Success: false, Message: "Payload required"}
		}
		hasValue := op.Payload.Value != nil
		hasTimeRange := op.Payload.TimestampFrom > 0 && op.Payload.TimestampTo > 0
		if !hasValue && !hasTimeRange {
			return Response{Success: false, Message: "Either value or both timestampFrom and timestampTo are required"}
		}
		if hasValue && op.Payload.Operator != ">" && op.Payload.Operator != "<" {
			return Response{Success: false, Message: "Payload operator must be '>' or '<' when value is provided"}
		}
		if (op.Payload.TimestampFrom > 0 && op.Payload.TimestampTo == 0) || (op.Payload.TimestampFrom == 0 && op.Payload.TimestampTo > 0) {
			return Response{Success: false, Message: "Both timestampFrom and timestampTo are required together"}
		}
		if op.Payload.TimestampFrom > 0 && op.Payload.TimestampTo > 0 && op.Payload.TimestampFrom > op.Payload.TimestampTo {
			return Response{Success: false, Message: "timestampFrom must be less than or equal to timestampTo"}
		}
		if !validateTimestamp(op.Payload.TimestampFrom) || !validateTimestamp(op.Payload.TimestampTo) {
			return Response{Success: false, Message: "Timestamp out of valid range (2000-2100)"}
		}

		value := 0.0
		if hasValue {
			value = *op.Payload.Value
		}
		removedCount := buffer.DeleteDataPoints(op.Key, op.Payload.Operator, value, hasValue, op.Payload.TimestampFrom, op.Payload.TimestampTo)
		return Response{
			Success: true,
			Message: fmt.Sprintf("Removed %d data points and patched data", removedCount),
		}

	default:
		return Response{Success: false, Message: "Invalid operation"}
	}
}
