package handlers

import (
	"gtsdb/fanout"
	"gtsdb/models"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// mockConn implements net.Conn interface for testing
type mockConn struct {
	io.Reader
	io.Writer
	closed bool
	br     *blockingReader
}

func (m *mockConn) Close() error {
	m.closed = true
	m.br.Close() // unblock the reader
	return nil
}
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// blockingReader reads the input string then blocks until Close is called
type blockingReader struct {
	data   string
	pos    int
	closed chan struct{}
}

func newBlockingReader(input string) *blockingReader {
	return &blockingReader{data: input, closed: make(chan struct{})}
}

func (r *blockingReader) Read(p []byte) (n int, err error) {
	if r.pos < len(r.data) {
		n = copy(p, r.data[r.pos:])
		r.pos += n
		return n, nil
	}
	// Block until Close is called, then return EOF
	<-r.closed
	return 0, io.EOF
}

func (r *blockingReader) Close() error {
	select {
	case <-r.closed:
	default:
		close(r.closed)
	}
	return nil
}

func newMockConn(input string) *mockConn {
	br := newBlockingReader(input)
	return &mockConn{
		Reader: br,
		Writer: &strings.Builder{},
		br:     br,
	}
}

func TestHandleTcpConnection(t *testing.T) {
	// shared capture for publish assertions (each subtest runs sequentially)
	var published models.DataPoint

	tests := []struct {
		name     string
		input    string
		wantErr  bool
		before   func(*fanout.Fanout)
		validate func(*testing.T, *fanout.Fanout)
	}{
		{
			name: "subscribe to device",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"subscribe","key":"dev1"}
`,
			validate: func(t *testing.T, f *fanout.Fanout) {
				if len(f.GetConsumers()) == 0 {
					t.Error("Expected consumer to be added")
				}
			},
		},
		{
			name: "subscribe and unsubscribe",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"subscribe","key":"dev1"}
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
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"write","key":"dev1","write":{"value":42.0}}
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
		{
			name: "adduser as root",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"adduser","key":"tcp_newuser"}
`,
		},
		{
			name: "adduser empty key auto-generate",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"adduser"}
`,
		},
		{
			name: "resetkey",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"adduser","key":"tcp_resetme"}
{"operation":"resetkey","key":"tcp_resetme"}
`,
		},
		{
			name: "invalid auth token",
			input: `{"operation":"auth","key":"bad-token"}
{"operation":"write","key":"x","write":{"value":1}}
`,
		},
		{
			name: "no auth before operation",
			input: `{"operation":"write","key":"x","write":{"value":1}}
`,
		},
		{
			name: "subscribe empty key",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"subscribe"}
`,
		},
		{
			name: "unsubscribe empty key",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"unsubscribe"}
`,
		},
		{
			name: "write with timestamp",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"write","key":"tcp_ts","write":{"value":99.9,"timestamp":2000000000}}
`,
			before: func(f *fanout.Fanout) {
				published = models.DataPoint{}
				f.AddConsumer(4242, func(dp models.DataPoint) {
					published = dp
				})
			},
			validate: func(t *testing.T, f *fanout.Fanout) {
				if published.Key != "root/tcp_ts" {
					t.Errorf("expected published key root/tcp_ts, got %s", published.Key)
				}
				if published.Timestamp != 2000000000 {
					t.Errorf("expected published timestamp 2000000000, got %d", published.Timestamp)
				}
				if published.Value != 99.9 {
					t.Errorf("expected published value 99.9, got %f", published.Value)
				}
			},
		},
		{
			name: "write without timestamp publishes resolved timestamp",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"write","key":"tcp_pub_ts","write":{"value":1.0}}
`,
			before: func(f *fanout.Fanout) {
				published = models.DataPoint{}
				f.AddConsumer(4243, func(dp models.DataPoint) {
					published = dp
				})
			},
			validate: func(t *testing.T, f *fanout.Fanout) {
				if published.Timestamp == 0 {
					t.Error("expected resolved non-zero timestamp in published point")
				}
				if published.Key != "root/tcp_pub_ts" || published.Value != 1.0 {
					t.Errorf("unexpected published point: %+v", published)
				}
			},
		},
		{
			name: "read lastx",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"write","key":"tcp_read","write":{"value":1}}
{"operation":"read","key":"tcp_read","read":{"lastx":1}}
`,
		},
		{
			name: "ids",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"ids"}
`,
		},
		{
			name: "initkey and deletekey",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"initkey","key":"tcp_init"}
{"operation":"deletekey","key":"tcp_init"}
`,
		},
		{
			name: "renamekey",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"initkey","key":"tcp_rename_src"}
{"operation":"renamekey","key":"tcp_rename_src","toKey":"tcp_rename_dst"}
`,
		},
		{
			name: "multi-read",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"write","key":"multi_a","write":{"value":1}}
{"operation":"write","key":"multi_b","write":{"value":2}}
{"operation":"multi-read","keys":["multi_a","multi_b"],"read":{"lastx":1}}
`,
		},
		{
			name: "data-patch",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"initkey","key":"tcp_patch"}
{"operation":"data-patch","key":"tcp_patch","data":"2000000000,1.5\\n2000000001,2.5"}
`,
		},
		{
			name: "export csv",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"write","key":"tcp_export","write":{"value":10}}
{"operation":"export","key":"tcp_export","export":{"format":"csv","lastx":1}}
`,
		},
		{
			name: "compact",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"write","key":"tcp_compact","write":{"value":1}}
{"operation":"compact","key":"tcp_compact"}
`,
		},
		{
			name: "flush",
			input: `{"operation":"auth","key":"` + testToken() + `"}
{"operation":"flush"}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newMockConn(tt.input)
			fanoutManager := fanout.NewFanout()
			if tt.before != nil {
				tt.before(fanoutManager)
			}

			done := make(chan struct{})
			go func() {
				HandleTcpConnection(conn, fanoutManager)
				close(done)
			}()

			// Give the handler time to process the input
			time.Sleep(50 * time.Millisecond)

			if tt.validate != nil {
				tt.validate(t, fanoutManager)
			}

			// Close the connection to unblock HandleTcpConnection
			conn.Close()

			// Wait for HandleTcpConnection to finish (with timeout)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("HandleTcpConnection did not exit after close")
			}
		})
	}
}
