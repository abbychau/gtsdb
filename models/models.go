package models

import "strconv"

type DataPoint struct {
	Key       string  `json:"key"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// MarshalJSON implements json.Marshaler with a fast, allocation-minimal path.
// Avoids reflection overhead for each of potentially millions of data points.
func (dp DataPoint) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 0, len(dp.Key)+64)
	buf = append(buf, `{"key":"`...)
	buf = append(buf, dp.Key...)
	buf = append(buf, `","timestamp":`...)
	buf = strconv.AppendInt(buf, dp.Timestamp, 10)
	buf = append(buf, `,"value":`...)
	buf = strconv.AppendFloat(buf, dp.Value, 'f', -1, 64)
	buf = append(buf, '}')
	return buf, nil
}

type IndexEntry struct {
	Timestamp int64 `json:"timestamp"`
	Offset    int64 `json:"offset"`
}

type KeyCount struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}
