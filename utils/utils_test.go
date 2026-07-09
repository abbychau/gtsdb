package utils

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestInitDataDirectory(t *testing.T) {
	DataDir = filepath.Join(os.TempDir(), "gtsdb_test")

	// Clean up after test
	defer os.RemoveAll(DataDir)

	InitDataDirectory()

	if _, err := os.Stat(DataDir); os.IsNotExist(err) {
		t.Errorf("Directory was not created: %v", err)
	}
}

func TestLoggingFunctions(t *testing.T) {
	// Set log level to Debug for testing
	oldLevel := atomic.LoadInt32(&LogLevel)
	atomic.StoreInt32(&LogLevel, LogLevelDebug)
	defer atomic.StoreInt32(&LogLevel, oldLevel)

	tests := []struct {
		name     string
		fn       interface{}
		message  interface{}
		expected string
	}{
		{"Log", Log, "test message", "🐹 test message"},
		{"Error", Error, "error message", "😡 error message"},
		{"Warning", Warning, "warning message", "😟 warning message"},
		{"Debug", Debug, "debug message", "🔍🐹 debug message"},
		{"Logln", Logln, "test message", "🐹 test message"},
		{"Errorln", Errorln, "error message", "😡 error message"},
		{"Warningln", Warningln, "warning message", "😟 warning message"},
		{"Debugln", Debugln, "debug message", "🔍🐹 debug message"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, w, _ := os.Pipe()
			old := os.Stdout
			os.Stdout = w

			switch f := tt.fn.(type) {
			case func(string, ...interface{}):
				f(tt.message.(string))
			case func(...interface{}):
				f(tt.message)
			}

			w.Close()
			os.Stdout = old

			out, _ := io.ReadAll(r)
			if !strings.Contains(string(out), tt.expected) {
				t.Errorf("Expected output containing %s, got %s", tt.expected, string(out))
			}
		})
	}
}

func TestPanic(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Defer cleanup
	defer func() {
		w.Close()
		os.Stdout = old
	}()

	// Test that Panic actually panics
	expectedMsg := "test panic message"
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		} else if r != expectedMsg {
			t.Errorf("Expected panic message %v, got %v", expectedMsg, r)
		}

		// Read captured output
		w.Close()
		os.Stdout = old
		out, _ := io.ReadAll(r)
		if !strings.Contains(string(out), "🚨🐹🚨") {
			t.Errorf("Expected output containing panic emoji, got %s", string(out))
		}
	}()

	Panic(expectedMsg)
}
