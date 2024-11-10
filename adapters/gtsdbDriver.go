package gtsdbclient

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	address    string
	conn       net.Conn
	mu         sync.Mutex
	reconnect  bool
	maxRetries int
}

type DataPoint struct {
	Timestamp int64
	Value     float64
}

type QueryResult struct {
	Key       string
	Timestamp int64
	Value     float64
}

const (
	defaultMaxRetries = 3
	reconnectDelay    = time.Second
)

// NewClient creates a new TSDB client with auto-reconnection
func NewClient(address string) *Client {
	return &Client{
		address:    address,
		reconnect:  true,
		maxRetries: defaultMaxRetries,
	}
}

func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *Client) ensureConnected() error {
	var err error
	for retry := 0; retry < c.maxRetries; retry++ {
		if c.conn != nil {
			// Test connection
			if _, err := c.conn.Write([]byte("")); err == nil {
				return nil
			}
			c.conn.Close()
			c.conn = nil
		}

		err = c.connect()
		if err == nil {
			return nil
		}

		time.Sleep(reconnectDelay)
	}
	return fmt.Errorf("failed to connect after %d retries: %w", c.maxRetries, err)
}

func (c *Client) writeCommand(cmd string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureConnected(); err != nil {
		return err
	}

	_, err := fmt.Fprintf(c.conn, "%s\n", cmd)
	return err
}

func (c *Client) readResponse() (string, error) {
	if err := c.ensureConnected(); err != nil {
		return "", err
	}

	reader := bufio.NewReader(c.conn)
	response, err := reader.ReadString('\n')
	return strings.TrimSpace(response), err
}

// StoreValue stores a single value for a key
func (c *Client) StoreValue(key string, value float64) error {
	timestamp := time.Now().Unix()
	return c.writeCommand(fmt.Sprintf("%s,%d,%.6f", key, timestamp, value))
}

// StoreValueAt stores a value for a key at a specific timestamp
func (c *Client) StoreValueAt(key string, timestamp time.Time, value float64) error {
	return c.writeCommand(fmt.Sprintf("%s,%d,%.6f", key, timestamp.Unix(), value))
}

// GetTimeRange retrieves data for a key within a time range with optional downsampling
func (c *Client) GetTimeRange(key string, start, end time.Time, downsampleSeconds int) ([]QueryResult, error) {
	cmd := fmt.Sprintf("%s,%d,%d,%d", key, start.Unix(), end.Unix(), downsampleSeconds)
	if err := c.writeCommand(cmd); err != nil {
		return nil, err
	}

	response, err := c.readResponse()
	if err != nil {
		return nil, err
	}

	return parseQueryResponse(response)
}

// GetLastValue retrieves the most recent value for a key
func (c *Client) GetLastValue(key string) (*QueryResult, error) {
	end := time.Now()
	start := end.Add(-time.Minute) // Small window to get last value
	results, err := c.GetTimeRange(key, start, end, 0)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no data found")
	}
	return &results[len(results)-1], nil
}

// GetAggregatedValue retrieves downsampled data for a key over a duration
func (c *Client) GetAggregatedValue(key string, duration time.Duration, downsampleSeconds int) ([]QueryResult, error) {
	end := time.Now()
	start := end.Add(-duration)
	return c.GetTimeRange(key, start, end, downsampleSeconds)
}

// Close closes the connection to the TSDB
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// Helper function to parse the query response
func parseQueryResponse(response string) ([]QueryResult, error) {
	if response == "" {
		return []QueryResult{}, nil
	}

	parts := strings.Split(response, "|")
	results := make([]QueryResult, 0, len(parts))

	for _, part := range parts {
		fields := strings.Split(part, ",")
		if len(fields) != 3 {
			continue
		}

		timestamp, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}

		value, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			continue
		}

		results = append(results, QueryResult{
			Key:       fields[0],
			Timestamp: timestamp,
			Value:     value,
		})
	}

	return results, nil
}
