//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gtsdb/auth"
	"gtsdb/buffer"
	"gtsdb/fanout"
	"gtsdb/handlers"
	"gtsdb/utils"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Test setup — spins up a real server with a temporary data directory
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	code := m.Run()
	if integrationServer != nil {
		integrationServer.Close()
	}
	if integrationDataDir != "" {
		os.RemoveAll(integrationDataDir)
	}
	os.Exit(code)
}

var (
	integrationBaseURL string
	integrationRootTok string
	integrationDataDir string
	integrationServer  *httptest.Server
	setupOnce          sync.Once
)

type intResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func integrationSetup(t *testing.T) {
	t.Helper()
	setupOnce.Do(func() {
		// Use a package-level temp dir
		var err error
		integrationDataDir, err = os.MkdirTemp("", "gtsdb-integration-*")
		if err != nil {
			t.Fatalf("mkdir temp: %v", err)
		}

		utils.DataDir = integrationDataDir
		utils.FileHandleLRUCapacity = 100
		utils.NoAuthUser = ""
		utils.RootToken = "integration-test-root-token-1234"

		utils.InitDataDirectory()
		buffer.InitFileHandles()
		buffer.InitIDSet()
		auth.Init(integrationDataDir)

		integrationRootTok = utils.RootToken

		// Start a real HTTP server via httptest
		fm := fanout.NewFanout()
		handler := handlers.SetupHTTPRoutes(fm)
		integrationServer = httptest.NewServer(handler)
		integrationBaseURL = integrationServer.URL
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func httpPost(t *testing.T, url, token string, body interface{}) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode: %v", err)
	}
	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http post: %v", err)
	}
	return resp
}

func httpGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	return resp
}

func decodeResp(t *testing.T, resp *http.Response) intResponse {
	t.Helper()
	defer resp.Body.Close()
	var r intResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return r
}

func writePoint(t *testing.T, key string, value float64, ts int64) {
	t.Helper()
	body := map[string]interface{}{
		"operation": "write",
		"key":       key,
		"write": map[string]interface{}{
			"value":     value,
			"timestamp": ts,
		},
	}
	resp := httpPost(t, integrationBaseURL+"/", integrationRootTok, body)
	r := decodeResp(t, resp)
	if !r.Success {
		t.Fatalf("write %s failed: %s", key, r.Message)
	}
}

// ---------------------------------------------------------------------------
// 1. Health & Metrics (no auth)
// ---------------------------------------------------------------------------

func TestIntegration_HealthEndpoint(t *testing.T) {
	integrationSetup(t)
	resp := httpGet(t, integrationBaseURL+"/health")
	if resp.StatusCode != 200 {
		t.Fatalf("health status %d", resp.StatusCode)
	}
	var m map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&m)
	resp.Body.Close()
	if m["status"] != "ok" {
		t.Errorf("health status = %v", m["status"])
	}
	if _, ok := m["keyCount"]; !ok {
		t.Error("health missing keyCount")
	}
}

func TestIntegration_MetricsEndpoint(t *testing.T) {
	integrationSetup(t)
	resp := httpGet(t, integrationBaseURL+"/metrics")
	if resp.StatusCode != 200 {
		t.Fatalf("metrics status %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("metrics content-type = %s", ct)
	}
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// 2. Write / Read flow
// ---------------------------------------------------------------------------

func TestIntegration_WriteRead(t *testing.T) {
	integrationSetup(t)
	key := "inttest_writeread"
	now := time.Now().Unix()

	// Write 10 points
	for i := 0; i < 10; i++ {
		writePoint(t, key, float64(i)*1.5, now+int64(i))
	}

	// Read last 5
	body := map[string]interface{}{
		"operation": "read",
		"key":       key,
		"read":      map[string]interface{}{"lastx": 5},
	}
	resp := httpPost(t, integrationBaseURL+"/", integrationRootTok, body)
	r := decodeResp(t, resp)
	if !r.Success {
		t.Fatalf("read failed: %s", r.Message)
	}
	var pts []struct {
		Key       string  `json:"key"`
		Timestamp int64   `json:"timestamp"`
		Value     float64 `json:"value"`
	}
	if err := json.Unmarshal(r.Data, &pts); err != nil {
		t.Fatalf("unmarshal points: %v", err)
	}
	if len(pts) != 5 {
		t.Errorf("expected 5 points, got %d", len(pts))
	}
	// Last point should be index 9
	if len(pts) > 0 && pts[len(pts)-1].Value != 9*1.5 {
		t.Errorf("last value = %f, want %f", pts[len(pts)-1].Value, 9*1.5)
	}

	// Read by time range
	body2 := map[string]interface{}{
		"operation": "read",
		"key":       key,
		"read": map[string]interface{}{
			"start_timestamp": now + 2,
			"end_timestamp":   now + 6,
			"aggregation":     "avg",
		},
	}
	resp2 := httpPost(t, integrationBaseURL+"/", integrationRootTok, body2)
	r2 := decodeResp(t, resp2)
	if !r2.Success {
		t.Fatalf("range read failed: %s", r2.Message)
	}
	var pts2 []struct {
		Key       string  `json:"key"`
		Timestamp int64   `json:"timestamp"`
		Value     float64 `json:"value"`
	}
	json.Unmarshal(r2.Data, &pts2)
	if len(pts2) < 3 {
		t.Errorf("expected >=3 points in range, got %d", len(pts2))
	}
}

// ---------------------------------------------------------------------------
// 3. Batch write
// ---------------------------------------------------------------------------

func TestIntegration_BatchWrite(t *testing.T) {
	integrationSetup(t)
	now := time.Now().Unix()

	body := map[string]interface{}{
		"operation": "batch-write",
		"points": []map[string]interface{}{
			{"key": "int_batch_1", "value": 1.0, "timestamp": now},
			{"key": "int_batch_2", "value": 2.0, "timestamp": now + 1},
			{"key": "int_batch_3", "value": 3.0, "timestamp": now + 2},
		},
	}
	resp := httpPost(t, integrationBaseURL+"/", integrationRootTok, body)
	r := decodeResp(t, resp)
	if !r.Success {
		t.Fatalf("batch-write failed: %s", r.Message)
	}

	// Verify each key has data
	for _, k := range []string{"int_batch_1", "int_batch_2", "int_batch_3"} {
		body2 := map[string]interface{}{
			"operation": "read",
			"key":       k,
			"read":      map[string]interface{}{"lastx": 1},
		}
		r2 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body2))
		if !r2.Success {
			t.Errorf("read %s after batch failed: %s", k, r2.Message)
		}
	}
}

// ---------------------------------------------------------------------------
// 4. Export
// ---------------------------------------------------------------------------

func TestIntegration_Export(t *testing.T) {
	integrationSetup(t)
	key := "inttest_export"
	now := time.Now().Unix()
	writePoint(t, key, 10, now)
	writePoint(t, key, 20, now+1)

	// Export JSON
	body := map[string]interface{}{
		"operation": "export",
		"key":       key,
		"export":    map[string]interface{}{"format": "json", "lastx": 2},
	}
	r := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body))
	if !r.Success {
		t.Fatalf("export json failed: %s", r.Message)
	}

	// Export CSV
	body2 := map[string]interface{}{
		"operation": "export",
		"key":       key,
		"export":    map[string]interface{}{"format": "csv", "lastx": 2},
	}
	r2 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body2))
	if !r2.Success {
		t.Fatalf("export csv failed: %s", r2.Message)
	}
	var csvStr string
	if err := json.Unmarshal(r2.Data, &csvStr); err != nil {
		t.Fatalf("csv data not a string: %v", err)
	}
	if !strings.HasPrefix(csvStr, "key,timestamp,value") {
		t.Errorf("csv missing header: %s", csvStr[:30])
	}
}

// ---------------------------------------------------------------------------
// 5. Data-patch
// ---------------------------------------------------------------------------

func TestIntegration_DataPatch(t *testing.T) {
	integrationSetup(t)
	key := "inttest_patch"
	now := time.Now().Unix()

	// Initial write
	writePoint(t, key, 100, now)

	// Patch with CSV
	csvData := fmt.Sprintf("%d,200\n%d,300\n%d,400", now+1, now+2, now+3)
	body := map[string]interface{}{
		"operation": "data-patch",
		"key":       key,
		"data":      csvData,
	}
	r := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body))
	if !r.Success {
		t.Fatalf("data-patch csv failed: %s", r.Message)
	}

	// Verify 4 points exist
	body2 := map[string]interface{}{
		"operation": "read",
		"key":       key,
		"read":      map[string]interface{}{"lastx": 10},
	}
	r2 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body2))
	if !r2.Success {
		t.Fatalf("read after patch failed: %s", r2.Message)
	}
	var pts []struct {
		Value float64 `json:"value"`
	}
	json.Unmarshal(r2.Data, &pts)
	if len(pts) != 4 {
		t.Errorf("expected 4 points after patch, got %d", len(pts))
	}
}

// ---------------------------------------------------------------------------
// 6. Delete data points
// ---------------------------------------------------------------------------

func TestIntegration_DeleteDataPoint(t *testing.T) {
	integrationSetup(t)
	key := "inttest_delete"
	now := time.Now().Unix()

	for i := 0; i < 5; i++ {
		writePoint(t, key, float64(i), now+int64(i))
	}

	// Delete where value > 2
	body := map[string]interface{}{
		"operation": "deleteDataPoint",
		"key":       key,
		"payload": map[string]interface{}{
			"operator": ">",
			"value":    2.0,
		},
	}
	r := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body))
	if !r.Success {
		t.Fatalf("deleteDataPoint failed: %s", r.Message)
	}

	// Read remaining
	body2 := map[string]interface{}{
		"operation": "read",
		"key":       key,
		"read":      map[string]interface{}{"lastx": 10},
	}
	r2 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body2))
	var pts []struct {
		Value float64 `json:"value"`
	}
	json.Unmarshal(r2.Data, &pts)
	if len(pts) != 3 {
		t.Errorf("expected 3 remaining points, got %d", len(pts))
	}
	for _, p := range pts {
		if p.Value > 2.0 {
			t.Errorf("value %f should have been deleted", p.Value)
		}
	}
}

// ---------------------------------------------------------------------------
// 7. Server info
// ---------------------------------------------------------------------------

func TestIntegration_ServerInfo(t *testing.T) {
	integrationSetup(t)
	body := map[string]interface{}{
		"operation": "serverinfo",
	}
	r := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body))
	if !r.Success {
		t.Fatalf("serverinfo failed: %s", r.Message)
	}
	var info map[string]interface{}
	json.Unmarshal(r.Data, &info)
	required := []string{"version", "key_count", "health", "uptime_seconds", "goroutines", "num_cpu"}
	for _, f := range required {
		if _, ok := info[f]; !ok {
			t.Errorf("serverinfo missing %s", f)
		}
	}
}

// ---------------------------------------------------------------------------
// 8. Key management: init, rename, ids, idsWithCount, delete, compact
// ---------------------------------------------------------------------------

func TestIntegration_KeyManagement(t *testing.T) {
	integrationSetup(t)
	key := "inttest_km"
	now := time.Now().Unix()
	writePoint(t, key, 42, now)

	// ids
	body := map[string]interface{}{"operation": "ids"}
	r := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body))
	if !r.Success {
		t.Fatalf("ids failed: %s", r.Message)
	}
	var ids []string
	json.Unmarshal(r.Data, &ids)
	if len(ids) == 0 {
		t.Error("ids returned empty")
	}

	// idswithcount
	body2 := map[string]interface{}{"operation": "idswithcount"}
	r2 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body2))
	if !r2.Success {
		t.Fatalf("idswithcount failed: %s", r2.Message)
	}

	// rename
	newKey := "inttest_km_renamed"
	body3 := map[string]interface{}{
		"operation": "renamekey",
		"key":       key,
		"tokey":     newKey,
	}
	r3 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body3))
	if !r3.Success {
		t.Fatalf("renamekey failed: %s", r3.Message)
	}

	// Read from new key
	body4 := map[string]interface{}{
		"operation": "read",
		"key":       newKey,
		"read":      map[string]interface{}{"lastx": 1},
	}
	r4 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body4))
	if !r4.Success {
		t.Errorf("read from renamed key failed: %s", r4.Message)
	}

	// compact
	body5 := map[string]interface{}{
		"operation": "compact",
		"key":       newKey,
	}
	r5 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body5))
	if !r5.Success {
		t.Fatalf("compact failed: %s", r5.Message)
	}

	// delete
	body6 := map[string]interface{}{
		"operation": "deletekey",
		"key":       newKey,
	}
	r6 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body6))
	if !r6.Success {
		t.Fatalf("deletekey failed: %s", r6.Message)
	}
}

// ---------------------------------------------------------------------------
// 9. Auth: user creation, token verification, isolation
// ---------------------------------------------------------------------------

func TestIntegration_AuthFlow(t *testing.T) {
	integrationSetup(t)

	// Create user via root
	body := map[string]interface{}{
		"operation": "adduser",
		"key":       "int_user_alice",
	}
	r := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body))
	if !r.Success {
		t.Fatalf("adduser failed: %s", r.Message)
	}
	var created struct {
		Name  string `json:"name"`
		Token string `json:"token"`
	}
	json.Unmarshal(r.Data, &created)
	if created.Name != "int_user_alice" {
		t.Errorf("expected name int_user_alice, got %s", created.Name)
	}
	if len(created.Token) != 32 {
		t.Errorf("expected token len 32, got %d", len(created.Token))
	}

	// Write data as new user
	aliceTok := created.Token
	writeBody := map[string]interface{}{
		"operation": "write",
		"key":       "alice_sensor",
		"write":     map[string]interface{}{"value": 99.9},
	}
	r2 := decodeResp(t, httpPost(t, integrationBaseURL+"/", aliceTok, writeBody))
	if !r2.Success {
		t.Fatalf("alice write failed: %s", r2.Message)
	}

	// Alice should see her own ids
	idsBody := map[string]interface{}{"operation": "ids"}
	r3 := decodeResp(t, httpPost(t, integrationBaseURL+"/", aliceTok, idsBody))
	if !r3.Success {
		t.Fatalf("alice ids failed: %s", r3.Message)
	}
	var aliceIDs []string
	json.Unmarshal(r3.Data, &aliceIDs)
	if len(aliceIDs) == 0 {
		t.Error("alice should see at least her own key")
	}

	// Reset alice's token
	resetBody := map[string]interface{}{
		"operation": "resetkey",
		"key":       "int_user_alice",
	}
	r4 := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, resetBody))
	if !r4.Success {
		t.Fatalf("resetkey failed: %s", r4.Message)
	}
	var resetResult struct {
		Token string `json:"token"`
	}
	json.Unmarshal(r4.Data, &resetResult)
	if resetResult.Token == aliceTok {
		t.Error("new token should differ from old token")
	}
}

// ---------------------------------------------------------------------------
// 10. TCP integration (connect, auth, write, read)
// ---------------------------------------------------------------------------

func tcpSend(t *testing.T, conn net.Conn, payload interface{}) intResponse {
	t.Helper()
	if err := json.NewEncoder(conn).Encode(payload); err != nil {
		t.Fatalf("tcp send: %v", err)
	}
	var r intResponse
	if err := json.NewDecoder(conn).Decode(&r); err != nil {
		t.Fatalf("tcp recv: %v", err)
	}
	return r
}

func TestIntegration_TCPFlow(t *testing.T) {
	integrationSetup(t)

	// Instead of spinning up a real TCP server (which requires running main),
	// test the TCP handler directly via a pipe.
	fm := fanout.NewFanout()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go handlers.HandleTcpConnection(server, fm)

	// Auth
	r := tcpSend(t, client, map[string]interface{}{
		"operation": "auth",
		"key":       integrationRootTok,
	})
	if !r.Success {
		t.Fatalf("tcp auth failed: %s", r.Message)
	}

	// Write
	r = tcpSend(t, client, map[string]interface{}{
		"operation": "write",
		"key":       "tcp_sensor",
		"write":     map[string]interface{}{"value": 77.7},
	})
	if !r.Success {
		t.Fatalf("tcp write failed: %s", r.Message)
	}

	// Read last 1
	r = tcpSend(t, client, map[string]interface{}{
		"operation": "read",
		"key":       "tcp_sensor",
		"read":      map[string]interface{}{"lastx": 1},
	})
	if !r.Success {
		t.Fatalf("tcp read failed: %s", r.Message)
	}

	// Subscribe (will fail because we'll close before getting data, but should ack)
	r = tcpSend(t, client, map[string]interface{}{
		"operation": "subscribe",
		"key":       "tcp_sensor",
	})
	if !r.Success {
		t.Fatalf("tcp subscribe failed: %s", r.Message)
	}

	// Unsubscribe
	r = tcpSend(t, client, map[string]interface{}{
		"operation": "unsubscribe",
		"key":       "tcp_sensor",
	})
	if !r.Success {
		t.Fatalf("tcp unsubscribe failed: %s", r.Message)
	}
}

// ---------------------------------------------------------------------------
// 11. Unauthorized access
// ---------------------------------------------------------------------------

func TestIntegration_Unauthorized(t *testing.T) {
	integrationSetup(t)

	// No auth header → 401 plain text
	body := map[string]interface{}{
		"operation": "write",
		"key":       "noauth_test",
		"write":     map[string]interface{}{"value": 1.0},
	}
	req, _ := http.NewRequest("POST", integrationBaseURL+"/", jsonBody(t, body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if resp.StatusCode != 401 {
		t.Errorf("expected 401 for no auth, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Invalid token → 401
	body2 := map[string]interface{}{
		"operation": "serverinfo",
	}
	resp2 := httpPost(t, integrationBaseURL+"/", "invalid-token-xxx", body2)
	if resp2.StatusCode != 401 {
		t.Errorf("expected 401 for invalid token, got %d", resp2.StatusCode)
	}
	resp2.Body.Close()
}

func jsonBody(t *testing.T, v interface{}) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(v)
	return &buf
}

// ---------------------------------------------------------------------------
// 12. Write at high frequency (stress-light)
// ---------------------------------------------------------------------------

func TestIntegration_WriteStress(t *testing.T) {
	integrationSetup(t)
	key := "inttest_stress"
	now := time.Now().Unix()

	// Write 500 points
	for i := 0; i < 500; i++ {
		writePoint(t, key, float64(i), now+int64(i))
	}

	// Read them all back
	body := map[string]interface{}{
		"operation": "read",
		"key":       key,
		"read":      map[string]interface{}{"lastx": 500},
	}
	r := decodeResp(t, httpPost(t, integrationBaseURL+"/", integrationRootTok, body))
	if !r.Success {
		t.Fatalf("stress read failed: %s", r.Message)
	}
	var pts []struct {
		Value float64 `json:"value"`
	}
	json.Unmarshal(r.Data, &pts)
	if len(pts) != 500 {
		t.Errorf("expected 500 points, got %d", len(pts))
	}
}

// ---------------------------------------------------------------------------
// 13. HTTP handler-level test (uses httptest.Server for fast isolated runs)
// ---------------------------------------------------------------------------

func TestIntegration_HTTPHandlerDirect(t *testing.T) {
	integrationSetup(t)

	fm := fanout.NewFanout()
	handler := handlers.SetupHTTPRoutes(fm)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Write via the test server
	body := map[string]interface{}{
		"operation": "write",
		"key":       "ht_test",
		"write":     map[string]interface{}{"value": 55.5},
	}
	// No auth → should get 401 since we removed NoAuthUser
	req, _ := http.NewRequest("POST", srv.URL+"/", jsonBody(t, body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if resp.StatusCode != 401 {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}
