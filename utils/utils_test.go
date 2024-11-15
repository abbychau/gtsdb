package utils

import (
	"io"
	"os"
	"path/filepath"
	"strings"
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
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	tests := []struct {
		name     string
		fn       func(string, ...interface{})
		message  string
		expected string
	}{
		{"Log", Log, "test message", "🐹 test message"},
		{"Error", Error, "error message", "😡 error message"},
		{"Warning", Warning, "warning message", "😟 warning message"},
		{"Debug", Debug, "debug message", "🔍🐹 debug message"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(tt.message)
			w.Close()
			out, _ := io.ReadAll(r)
			if !strings.Contains(string(out), tt.expected) {
				t.Errorf("Expected output containing %s, got %s", tt.expected, string(out))
			}
			// Reset for next test
			r, w, _ = os.Pipe()
			os.Stdout = w
		})
	}

	// Restore stdout
	os.Stdout = old
}
