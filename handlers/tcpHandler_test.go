package handlers

import (
	"bufio"
	"gtsdb/fanout"
	"net"
	"strings"
	"testing"
	"time"
)

// mockConn implements net.Conn interface for testing
type mockConn struct {
	*bufio.Reader
	*bufio.Writer
	closed bool
}

func (m *mockConn) Close() error                       { m.closed = true; return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func newMockConn(input string) *mockConn {
	return &mockConn{
		Reader: bufio.NewReader(strings.NewReader(input)),
		Writer: bufio.NewWriter(&strings.Builder{}),
	}
}

func TestHandleTcpConnection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		validate func(*testing.T, *fanout.Fanout)
	}{
		{
			name: "subscribe to device",
			input: `{"operation":"subscribe","key":"dev1"}
`,
			validate: func(t *testing.T, f *fanout.Fanout) {
				if len(f.GetConsumers()) == 0 {
					t.Error("Expected consumer to be added")
				}
			},
		},
		{
			name: "subscribe and unsubscribe",
			input: `{"operation":"subscribe","key":"dev1"}
{"operation":"unsubscribe","key":"dev1"}
`,
			validate: func(t *testing.T, f *fanout.Fanout) {
				if len(f.GetConsumers()) != 0 {
					t.Error("Expected consumer to be removed")
				}
			},
		},
		{
			name: "write operation",
			input: `{"operation":"write","key":"dev1","write":{"value":42.0}}
`,
			validate: func(t *testing.T, f *fanout.Fanout) {
				// Validation happens through response in real connection
			},
		},
		{
			name: "invalid json",
			input: `{"invalid json"
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newMockConn(tt.input)
			fanoutManager := fanout.NewFanout(10) // Buffer size of 10 for handler tests

			HandleTcpConnection(conn, fanoutManager)

			if !conn.closed {
				t.Error("Connection was not closed")
			}

			if tt.validate != nil {
				tt.validate(t, fanoutManager)
			}
		})
	}
}
