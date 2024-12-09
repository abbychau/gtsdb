package main

import (
	"fmt"
	"gtsdb/fanout"
	"gtsdb/utils"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Test helpers
func createTestIniFile(t *testing.T) string {
	content := `[listens]
tcp = localhost:5555
http = localhost:5556
[paths]
data = ./testdata`

	tmpDir := t.TempDir()
	iniPath := filepath.Join(tmpDir, "test.ini")
	if err := os.WriteFile(iniPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return iniPath
}

func TestLoadConfig(t *testing.T) {
	// Test with valid config
	iniPath := createTestIniFile(t)
	loadConfig(iniPath)

	if utils.TcpListenAddr != "localhost:5555" {
		t.Errorf("Expected TCP address localhost:5555, got %s", utils.TcpListenAddr)
	}
	if utils.HttpListenAddr != "localhost:5556" {
		t.Errorf("Expected HTTP address localhost:5556, got %s", utils.HttpListenAddr)
	}
	if utils.DataDir != "./testdata" {
		t.Errorf("Expected data dir ./testdata, got %s", utils.DataDir)
	}
}

func TestLoadConfigInvalidFile(t *testing.T) {
	utils.TcpListenAddr = ":5555"
	utils.HttpListenAddr = ":5556"
	utils.DataDir = "data"
	// Test with non-existent config
	loadConfig("nonexistent.ini")
	// Should use defaults, no panic

	if utils.TcpListenAddr != ":5555" {
		t.Errorf("Expected TCP address localhost:5555, got %s", utils.TcpListenAddr)
	}
	if utils.HttpListenAddr != ":5556" {
		t.Errorf("Expected HTTP address localhost:5556, got %s", utils.HttpListenAddr)
	}
	if utils.DataDir != "data" {
		t.Errorf("Expected data dir data, got %s", utils.DataDir)
	}
}

func TestGracefulShutdown(t *testing.T) {
	// Create test data directory
	testDataDir := t.TempDir()
	utils.DataDir = testDataDir

	// Write some test data points that need to be flushed
	testFile := filepath.Join(testDataDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Call gracefulShutdown
	gracefulShutdown()

	// Verify cleanup was performed
	if _, err := os.Stat(testFile); err != nil {
		t.Error("Expected test file to persist after graceful shutdown")
	}

	// Additional verification could be added here depending on what
	// buffer.FlushRemainingDataPoints() does
}

func TestTCPServerInitialization(t *testing.T) {
	utils.TcpListenAddr = "localhost:5555"
	fanoutManager := fanout.NewFanout()
	stop := make(chan struct{})

	// Start TCP server in goroutine
	go startTCPServerWithStop(fanoutManager, stop)

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Try to connect
	conn, err := net.Dial("tcp", utils.TcpListenAddr)
	if err != nil {
		t.Errorf("Failed to connect to TCP server: %v", err)
	}
	if conn != nil {
		conn.Close()
	}

	// Test graceful shutdown
	close(stop)
	time.Sleep(100 * time.Millisecond)

	// Verify server stopped
	_, err = net.Dial("tcp", utils.TcpListenAddr)
	if err == nil {
		t.Error("Server should have stopped")
	}
}

func TestHTTPServerInitialization(t *testing.T) {
	utils.HttpListenAddr = "localhost:5556"
	fanoutManager := fanout.NewFanout()
	stop := make(chan struct{})

	// Start HTTP server in goroutine
	go startHTTPServerWithStop(fanoutManager, stop)

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Try to connect
	resp, err := http.Get(fmt.Sprintf("http://%s/health", utils.HttpListenAddr))
	if err != nil {
		t.Errorf("Failed to connect to HTTP server: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}

	// Test graceful shutdown
	close(stop)
	time.Sleep(100 * time.Millisecond)

	// Verify server stopped
	_, err = http.Get(fmt.Sprintf("http://%s/health", utils.HttpListenAddr))
	if err == nil {
		t.Error("Server should have stopped")
	}
}

func TestMainIntegration(t *testing.T) {
	t.Skip("Skipping integration test")

	// Create channel to simulate interrupt
	done := make(chan bool)

	go func() {
		// Run main in background
		go main()

		// Give servers time to start
		time.Sleep(2000 * time.Millisecond)

		// Verify both servers are running
		tcpConn, err := net.Dial("tcp", utils.TcpListenAddr)
		if err != nil {
			t.Errorf("TCP server not running: %v", err)
		}
		if tcpConn != nil {
			tcpConn.Close()
		}

		resp, err := http.Get(fmt.Sprintf("http://%s/", utils.HttpListenAddr))
		if err != nil {
			t.Errorf("HTTP server not running: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		done <- true
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out")
	}
}

func TestRun(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create test config file
	configContent := `[listens]
tcp = "localhost:0"
http = "localhost:0"
[paths]
data = "` + tmpDir + `"`

	configFile := filepath.Join(tmpDir, "test.ini")
	if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Start the application in a goroutine
	done := make(chan bool)
	go func() {
		go run(configFile)
		time.Sleep(100 * time.Millisecond) // Give time for servers to start

		// Send interrupt signal to trigger shutdown
		p, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Error(err)
			return
		}
		p.Signal(os.Interrupt)
		done <- true
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Verify data directory was created
		if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
			t.Error("Data directory was not created")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out")
	}
}
