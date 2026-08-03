package main

import (
	"flag"
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
// createTestIniFile writes a config file pointing at a fresh temp data dir
// and returns the ini path plus the data dir it references.
func createTestIniFile(t *testing.T) (string, string) {
	dataDir := t.TempDir()
	content := fmt.Sprintf(`[listens]
tcp = localhost:5555
http = localhost:5556
[paths]
data = %s`, dataDir)

	tmpDir := t.TempDir()
	iniPath := filepath.Join(tmpDir, "test.ini")
	if err := os.WriteFile(iniPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return iniPath, dataDir
}

func TestLoadConfig(t *testing.T) {
	// Test with valid config
	iniPath, dataDir := createTestIniFile(t)
	loadConfig(iniPath)

	if utils.TcpListenAddr != "localhost:5555" {
		t.Errorf("Expected TCP address localhost:5555, got %s", utils.TcpListenAddr)
	}
	if utils.HttpListenAddr != "localhost:5556" {
		t.Errorf("Expected HTTP address localhost:5556, got %s", utils.HttpListenAddr)
	}
	if utils.DataDir != dataDir {
		t.Errorf("Expected data dir %s, got %s", dataDir, utils.DataDir)
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
}

// freePort reserves a free TCP port and releases it, returning "host:port".
// Preferred over hard-coded ports, which collide when tests run in parallel
// or when the port is already taken on the machine.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestTCPServerInitialization(t *testing.T) {
	utils.TcpListenAddr = freePort(t)
	fanoutManager := fanout.NewFanout()
	stop := make(chan struct{})

	// Start TCP server in goroutine
	go startTCPServerWithStop(utils.TcpListenAddr, utils.NoAuthUser, fanoutManager, stop)

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

func TestTCPServerWithInvalidAddress(t *testing.T) {
	utils.TcpListenAddr = "invalid:address:format"
	fanoutManager := fanout.NewFanout()
	stop := make(chan struct{})

	// Start TCP server with invalid address
	startTCPServerWithStop(utils.TcpListenAddr, utils.NoAuthUser, fanoutManager, stop)

	// Should return without panic
	close(stop)
}

func TestHTTPServerInitialization(t *testing.T) {
	utils.HttpListenAddr = freePort(t)
	fanoutManager := fanout.NewFanout()
	stop := make(chan struct{})

	// Start HTTP server in goroutine
	go startHTTPServerWithStop(utils.HttpListenAddr, utils.NoAuthUser, fanoutManager, stop)

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

func TestHTTPServerWithInvalidAddress(t *testing.T) {
	utils.HttpListenAddr = "invalid:address:format"
	fanoutManager := fanout.NewFanout()
	stop := make(chan struct{})

	// Start HTTP server with invalid address
	startHTTPServerWithStop(utils.HttpListenAddr, utils.NoAuthUser, fanoutManager, stop)

	// Should return without panic
	close(stop)
}

func TestMainArgs(t *testing.T) {
	// Save and restore original args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Create temp config file
	tmpDir := t.TempDir()
	customConfig := filepath.Join(tmpDir, "custom.ini")
	content := `[listens]
tcp = "localhost:0"
http = "localhost:0"
[paths]
data = "` + tmpDir + `"`

	if err := os.WriteFile(customConfig, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Test cases
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{
			name: "custom config",
			args: []string{"cmd", customConfig},
		},
		{
			name: "missing config falls back to default",
			args: []string{"cmd"},
		},
		{
			name: "nonexistent config",
			args: []string{"cmd", "nonexistent.ini"},
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set args for this test
			os.Args = tt.args

			// Create default config if testing default path
			if len(tt.args) == 1 {
				if err := os.WriteFile("gtsdb.ini", []byte(content), 0644); err != nil {
					t.Fatal(err)
				}
				defer os.Remove("gtsdb.ini")
			}

			// Call the function directly and verify it doesn't panic
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("main panicked: %v", r)
					}
				}()

				flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
				flag.Parse()
				configFile := "gtsdb.ini"
				if args := flag.Args(); len(args) > 0 {
					configFile = args[0]
				}
				loadConfig(configFile)
			}()
		})
	}
}

func TestMainIntegration(t *testing.T) {
	// Create temporary config file
	configPath, _ := createTestIniFile(t)

	// Run main with custom config in background
	done := make(chan bool)
	go func() {
		os.Args = []string{"cmd", configPath}
		go main()

		// Give servers time to start
		time.Sleep(100 * time.Millisecond)

		// Verify both servers are running
		tcpConn, err := net.Dial("tcp", "localhost:5555")
		if err != nil {
			t.Errorf("TCP server not running: %v", err)
		}
		if tcpConn != nil {
			tcpConn.Close()
		}

		resp, err := http.Get("http://localhost:5556/health")
		if err != nil {
			t.Errorf("HTTP server not running: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		// Send interrupt signal
		p, _ := os.FindProcess(os.Getpid())
		_ = p.Signal(os.Interrupt)

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
		_ = p.Signal(os.Interrupt)
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
