package models

type DataPoint struct {
	ID        string  `json:"id"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type IndexEntry struct {
	Timestamp int64 `json:"timestamp"`
	Offset    int64 `json:"offset"`
}

type KeyCount struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}
