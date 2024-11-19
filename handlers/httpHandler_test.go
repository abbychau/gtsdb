package handlers

import (
	"bytes"
	"encoding/json"
	"gtsdb/fanout"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSetupHTTPRoutes(t *testing.T) {
	fanoutManager := fanout.NewFanout()
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
				Write: &WriteRequest{
					ID:    "test1",
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
				Read: &ReadRequest{
					ID:          "test1",
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
