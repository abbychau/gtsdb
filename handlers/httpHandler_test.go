package handlers

import (
	"bytes"
	"encoding/json"
	"gtsdb/fanout"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSetupHTTPRoutes(t *testing.T) {
	fanoutManager := fanout.NewFanout(10) // Buffer size of 10 for handler tests
	handler := SetupHTTPRoutes(fanoutManager)

	tests := []struct {
		name           string
		method         string
		operation      Operation
		expectedStatus int
		expectSuccess  bool
	}{
		{
			name:           "Method not allowed",
			method:         "GET",
			expectedStatus: http.StatusOK,
			expectSuccess:  false,
		},
		{
			name:   "Write operation",
			method: "POST",
			operation: Operation{
				Operation: "write",

				Key: "test1",
				Write: &WriteRequest{
					Value: 42.0,
				},
			},
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
		{
			name:   "Read operation",
			method: "POST",
			operation: Operation{
				Operation: "read",

				Key: "test1",
				Read: &ReadRequest{
					LastX:       1,
					Aggregation: "avg",
				},
			},
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
		{
			name:   "Flush operation",
			method: "POST",
			operation: Operation{
				Operation: "flush",
			},
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body []byte
			var err error
			if tt.method == "POST" {
				body, err = json.Marshal(tt.operation)
				if err != nil {
					t.Fatal(err)
				}
			}

			req := httptest.NewRequest(tt.method, "/", bytes.NewBuffer(body))
			req.Header.Set("Authorization", "Bearer "+testToken())
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					rr.Code, tt.expectedStatus)
			}

			var response Response
			if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
				t.Fatal(err)
			}

			if response.Success != tt.expectSuccess {
				t.Errorf("handler returned wrong success status: got %v want %v",
					response.Success, tt.expectSuccess)
			}
		})
	}
}

func TestHealthEndpoint(t *testing.T) {
	fanoutManager := fanout.NewFanout(10)
	handler := SetupHTTPRoutes(fanoutManager)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("health endpoint returned %d, expected %d", rr.Code, http.StatusOK)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	expectedFields := []string{"status", "service", "version", "keyCount"}
	for _, field := range expectedFields {
		if _, exists := result[field]; !exists {
			t.Errorf("health response missing field: %s", field)
		}
	}
	if result["status"] != "ok" {
		t.Errorf("expected status 'ok', got %v", result["status"])
	}
}

func TestMetricsEndpoint(t *testing.T) {
	fanoutManager := fanout.NewFanout(10)
	handler := SetupHTTPRoutes(fanoutManager)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("metrics endpoint returned %d, expected %d", rr.Code, http.StatusOK)
	}

	contentType := rr.Header().Get("Content-Type")
	if contentType != "text/plain; version=0.0.4" {
		t.Errorf("expected Content-Type 'text/plain; version=0.0.4', got %s", contentType)
	}

	body := rr.Body.String()
	expectedMetrics := []string{"gtsdb_key_count", "gtsdb_data_points_total", "gtsdb_uptime_seconds", "go_memstats_alloc_bytes"}
	for _, metric := range expectedMetrics {
		if !containsMetric(body, metric) {
			t.Errorf("metrics response missing: %s", metric)
		}
	}
}

// containsMetric checks if a Prometheus metric name appears in the response body
func containsMetric(body, metric string) bool {
	return strings.Contains(body, metric)
}

func TestHTTPMoreOperations(t *testing.T) {
	fanoutManager := fanout.NewFanout(10)
	handler := SetupHTTPRoutes(fanoutManager)
	token := testToken()

	doPost := func(op Operation) *httptest.ResponseRecorder {
		body, _ := json.Marshal(op)
		req := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		return rr
	}

	// First write some data
	warmup := Operation{Operation: "write", Key: "http_test", Write: &WriteRequest{Value: 1.0}}
	doPost(warmup)

	t.Run("batch-write", func(t *testing.T) {
		op := Operation{
			Operation: "batch-write",
			Points: []BatchWritePoint{
				{Key: "http_batch_a", Value: 1.0},
				{Key: "http_batch_b", Value: 2.0},
			},
		}
		rr := doPost(op)
		if rr.Code != http.StatusOK {
			t.Errorf("batch-write returned %d", rr.Code)
		}
	})

	t.Run("export JSON", func(t *testing.T) {
		op := Operation{
			Operation: "export",
			Key:       "http_test",
			Export:    &ExportRequest{Format: "json", LastX: 1},
		}
		rr := doPost(op)
		if rr.Code != http.StatusOK {
			t.Errorf("export returned %d", rr.Code)
		}
	})

	t.Run("ids", func(t *testing.T) {
		rr := doPost(Operation{Operation: "ids"})
		if rr.Code != http.StatusOK {
			t.Errorf("ids returned %d", rr.Code)
		}
	})

	t.Run("flush", func(t *testing.T) {
		rr := doPost(Operation{Operation: "flush"})
		if rr.Code != http.StatusOK {
			t.Errorf("flush returned %d", rr.Code)
		}
	})

	t.Run("serverinfo", func(t *testing.T) {
		rr := doPost(Operation{Operation: "serverinfo"})
		if rr.Code != http.StatusOK {
			t.Errorf("serverinfo returned %d", rr.Code)
		}
	})

	t.Run("compact", func(t *testing.T) {
		rr := doPost(Operation{Operation: "compact", Key: "http_test"})
		if rr.Code != http.StatusOK {
			t.Errorf("compact returned %d", rr.Code)
		}
	})

	t.Run("initkey", func(t *testing.T) {
		rr := doPost(Operation{Operation: "initkey", Key: "http_init"})
		if rr.Code != http.StatusOK {
			t.Errorf("initkey returned %d", rr.Code)
		}
	})

	t.Run("deletekey", func(t *testing.T) {
		doPost(Operation{Operation: "initkey", Key: "http_delete_me"})
		rr := doPost(Operation{Operation: "deletekey", Key: "http_delete_me"})
		if rr.Code != http.StatusOK {
			t.Errorf("deletekey returned %d", rr.Code)
		}
	})

	t.Run("renamekey", func(t *testing.T) {
		doPost(Operation{Operation: "initkey", Key: "http_rename_src"})
		rr := doPost(Operation{Operation: "renamekey", Key: "http_rename_src", ToKey: "http_rename_dst"})
		if rr.Code != http.StatusOK {
			t.Errorf("renamekey returned %d", rr.Code)
		}
	})

	t.Run("data-patch", func(t *testing.T) {
		doPost(Operation{Operation: "initkey", Key: "http_patch"})
		rr := doPost(Operation{
			Operation: "data-patch",
			Key:       "http_patch",
			Data:      "2000000000,1.5\n2000000001,2.5",
		})
		if rr.Code != http.StatusOK {
			t.Errorf("data-patch returned %d", rr.Code)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		rr := doPost(Operation{Operation: "write"})
		if rr.Code != http.StatusOK {
			t.Errorf("missing key returned %d", rr.Code)
		}
	})
}
