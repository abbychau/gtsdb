package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type mockConn struct {
	net.Conn
	reader *strings.Reader
	writer strings.Builder
}

func (m *mockConn) Read(p []byte) (n int, err error) {
	return m.reader.Read(p)
}

func (m *mockConn) Write(p []byte) (n int, err error) {
	return m.writer.Write(p)
}

func (m *mockConn) Close() error {
	return nil
}

func TestHandleConnection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "write valid data point",
			input:    "write,sensor1,1234567890,23.45\n",
			expected: "Data point stored\n",
		},
		{
			name:     "write invalid format",
			input:    "write,sensor1\n",
			expected: "Invalid write format\n",
		},
		{
			name:     "read valid request",
			input:    "read,sensor1,1234567890,1234567899,1\n",
			expected: "", // Empty is ok as there might be no data
		},
		{
			name:     "read invalid format",
			input:    "read,sensor1\n",
			expected: "Invalid read format\n",
		},
		{
			name:     "subscribe valid",
			input:    "subscribe,sensor1\n",
			expected: "msg:Subscribed to sensor1\n",
		},
		{
			name:     "unsubscribe valid",
			input:    "unsubscribe,sensor1\n",
			expected: "msg:Unsubscribed from sensor1\n",
		},
		{
			name:     "invalid action",
			input:    "invalid,sensor1\n",
			expected: "Invalid action\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockConn{
				reader: strings.NewReader(tt.input),
				writer: strings.Builder{},
			}

			done := make(chan bool)
			go func() {
				handleConnection(mock)
				done <- true
			}()

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Test timed out")
			}

			if tt.expected != "" && !strings.Contains(mock.writer.String(), tt.expected) {
				t.Errorf("Expected response containing %q, got %q", tt.expected, mock.writer.String())
			}
		})
	}
}

func clearData() error {
	files, err := filepath.Glob("data/*")
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return err
		}
	}
	return nil
}

func TestHandleConnectionIntegration(t *testing.T) {
	if err := clearData(); err != nil {
		t.Fatalf("Failed to clear data directory: %v", err)
	}

	client, server := net.Pipe()
	defer client.Close()

	go handleConnection(server)

	// Write test
	fmt.Fprintf(client, "write,sensor1,1234567890,23.45\n")
	response, _ := bufio.NewReader(client).ReadString('\n')
	if response != "Data point stored\n" {
		t.Errorf("Expected 'Data point stored', got %q", response)
	}

	// flush db (cmd: flush)
	fmt.Fprintf(client, "flush\n")
	response, _ = bufio.NewReader(client).ReadString('\n')
	if response != "Data points flushed\n" {
		t.Errorf("Expected 'Data points flushed', got %q", response)
	}

	// Read test
	fmt.Fprintf(client, "read,sensor1,1234567889,1234567899,1\n")
	response, _ = bufio.NewReader(client).ReadString('\n')
	if !strings.Contains(response, "sensor1") {
		t.Errorf("Expected response to contain 'sensor1', got %q", response)
	}

	// Subscribe test
	fmt.Fprintf(client, "subscribe,sensor1\n")
	response, _ = bufio.NewReader(client).ReadString('\n')
	if response != "msg:Subscribed to sensor1\n" {
		t.Errorf("Expected subscribe confirmation, got %q", response)
	}

	// Unsubscribe test
	fmt.Fprintf(client, "unsubscribe,sensor1\n")
	response, _ = bufio.NewReader(client).ReadString('\n')
	if response != "msg:Unsubscribed from sensor1\n" {
		t.Errorf("Expected unsubscribe confirmation, got %q", response)
	}
}
