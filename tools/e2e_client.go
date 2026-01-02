package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

var (
	baseURL = "http://127.0.0.1:5556"
	tcpAddr = "127.0.0.1:5555"
)

type Response struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type User struct {
	Name  string `json:"name"`
	Token string `json:"token"`
}

type DataPoint struct {
	Key       string  `json:"key"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type Operation struct {
	Operation string      `json:"operation"`
	Key       string      `json:"key,omitempty"`
	Write     *DataPoint  `json:"write,omitempty"`
	Read      *ReadParams `json:"read,omitempty"`
}

type ReadParams struct {
	LastX int `json:"lastx,omitempty"`
}

func main() {
	usersFile := flag.String("users", "mydata/users.json", "Path to users.json file")
	flag.Parse()

	fmt.Println("🚀 Starting E2E Test Client...")

	// 1. Get Root Token
	rootToken, err := getRootToken(*usersFile)
	if err != nil {
		fmt.Printf("❌ Failed to get root token: %v\n", err)
		fmt.Println("Make sure the server is running and 'mydata/users.json' exists.")
		os.Exit(1)
	}
	fmt.Printf("🔑 Got root token: %s...\n", rootToken[:8])

	// 2. Create Users
	alice, err := setupUser(rootToken, "alice")
	if err != nil {
		fmt.Printf("❌ Failed to setup alice: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("👤 Setup Alice: %s...\n", alice.Token[:8])

	bob, err := setupUser(rootToken, "bob")
	if err != nil {
		fmt.Printf("❌ Failed to setup bob: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("👤 Setup Bob:   %s...\n", bob.Token[:8])

	// 3. HTTP Isolation Test
	fmt.Println("\n🧪 Running HTTP Isolation Test...")
	if err := runHttpTest(alice, bob); err != nil {
		fmt.Printf("❌ HTTP Test Failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✅ HTTP Isolation Test Passed")

	// 4. TCP Isolation Test
	fmt.Println("\n🧪 Running TCP Isolation Test...")
	if err := runTcpTest(alice, bob); err != nil {
		fmt.Printf("❌ TCP Test Failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✅ TCP Isolation Test Passed")

	fmt.Println("\n🎉 All tests passed successfully!")
}

func getRootToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var users []User
	if err := json.Unmarshal(data, &users); err != nil {
		return "", err
	}
	for _, u := range users {
		if u.Name == "root" {
			return u.Token, nil
		}
	}
	return "", fmt.Errorf("root user not found in %s", path)
}

func setupUser(rootToken, name string) (User, error) {
	// Try to create
	u, err := createUser(rootToken, name)
	if err == nil {
		return u, nil
	}
	// If fails, try to reset token (assuming it failed because user exists)
	token, err := resetUser(rootToken, name)
	if err != nil {
		return User{}, err
	}
	return User{Name: name, Token: token}, nil
}

func createUser(rootToken, name string) (User, error) {
	op := Operation{Operation: "adduser", Key: name}
	reqBody, _ := json.Marshal(op)
	req, _ := http.NewRequest("POST", baseURL+"/", bytes.NewBuffer(reqBody))
	req.Header.Set("Authorization", "Bearer "+rootToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return User{}, err
	}
	defer resp.Body.Close()

	var r Response
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return User{}, err
	}
	if !r.Success {
		return User{}, fmt.Errorf(r.Message)
	}

	var u User
	if err := json.Unmarshal(r.Data, &u); err != nil {
		return User{}, err
	}
	return u, nil
}

func resetUser(rootToken, name string) (string, error) {
	op := Operation{Operation: "resetkey", Key: name}
	reqBody, _ := json.Marshal(op)
	req, _ := http.NewRequest("POST", baseURL+"/", bytes.NewBuffer(reqBody))
	req.Header.Set("Authorization", "Bearer "+rootToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var r Response
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", err
	}
	if !r.Success {
		return "", fmt.Errorf(r.Message)
	}

	var data map[string]string
	if err := json.Unmarshal(r.Data, &data); err != nil {
		return "", err
	}
	return data["token"], nil
}

func httpWrite(user User, key string, value float64) error {
	op := Operation{
		Operation: "write",
		Key:       key,
		Write:     &DataPoint{Value: value},
	}
	body, _ := json.Marshal(op)
	req, _ := http.NewRequest("POST", baseURL+"/", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+user.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var r Response
	json.NewDecoder(resp.Body).Decode(&r)
	if !r.Success {
		return fmt.Errorf(r.Message)
	}
	return nil
}

func httpRead(user User, key string) (float64, error) {
	op := Operation{
		Operation: "read",
		Key:       key,
		Read:      &ReadParams{LastX: 1},
	}
	body, _ := json.Marshal(op)
	req, _ := http.NewRequest("POST", baseURL+"/", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+user.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var r Response
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, err
	}
	if !r.Success {
		return 0, fmt.Errorf(r.Message)
	}

	var points []DataPoint
	if err := json.Unmarshal(r.Data, &points); err != nil {
		return 0, err
	}
	if len(points) == 0 {
		return 0, fmt.Errorf("no data")
	}
	return points[0].Value, nil
}

func runHttpTest(alice, bob User) error {
	key := fmt.Sprintf("http_metric_%d", time.Now().UnixNano())

	// Alice writes 100
	if err := httpWrite(alice, key, 100); err != nil {
		return fmt.Errorf("alice write failed: %v", err)
	}

	// Bob reads (should fail or be empty)
	if _, err := httpRead(bob, key); err == nil {
		// If it didn't error, check if it returned data.
		// Our httpRead returns error if "no data".
		// So if err == nil, Bob saw something.
		return fmt.Errorf("bob saw alice's data")
	}

	// Bob writes 200
	if err := httpWrite(bob, key, 200); err != nil {
		return fmt.Errorf("bob write failed: %v", err)
	}

	// Alice reads (should be 100)
	val, err := httpRead(alice, key)
	if err != nil {
		return fmt.Errorf("alice read failed: %v", err)
	}
	if val != 100 {
		return fmt.Errorf("alice read wrong value: got %v, want 100", val)
	}

	// Bob reads (should be 200)
	val, err = httpRead(bob, key)
	if err != nil {
		return fmt.Errorf("bob read failed: %v", err)
	}
	if val != 200 {
		return fmt.Errorf("bob read wrong value: got %v, want 200", val)
	}

	return nil
}

func runTcpTest(alice, bob User) error {
	// Alice connects
	connAlice, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		return err
	}
	defer connAlice.Close()
	decAlice := json.NewDecoder(connAlice)

	// Bob connects
	connBob, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		return err
	}
	defer connBob.Close()
	decBob := json.NewDecoder(connBob)

	// Auth Alice
	if err := tcpAuth(connAlice, decAlice, alice.Token); err != nil {
		return fmt.Errorf("alice auth failed: %v", err)
	}

	// Auth Bob
	if err := tcpAuth(connBob, decBob, bob.Token); err != nil {
		return fmt.Errorf("bob auth failed: %v", err)
	}

	key := fmt.Sprintf("tcp_live_%d", time.Now().UnixNano())

	// Alice subscribes
	subOp := Operation{Operation: "subscribe", Key: key}
	if err := json.NewEncoder(connAlice).Encode(subOp); err != nil {
		return err
	}
	// Read subscribe confirmation
	if _, err := readTcpResponse(decAlice); err != nil {
		return err
	}

	// Bob writes 999
	writeOp := Operation{Operation: "write", Key: key, Write: &DataPoint{Value: 999}}
	if err := json.NewEncoder(connBob).Encode(writeOp); err != nil {
		return err
	}
	readTcpResponse(decBob) // Consume write confirmation

	// Alice should NOT receive anything immediately.
	// We'll set a short read deadline on Alice to verify no data comes.
	/*
		connAlice.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		if _, err := readTcpResponse(decAlice); err == nil {
			return fmt.Errorf("alice received bob's data")
		}
		connAlice.SetReadDeadline(time.Time{}) // Reset deadline
	*/

	// Alice writes 111
	writeOpAlice := Operation{Operation: "write", Key: key, Write: &DataPoint{Value: 111}}
	if err := json.NewEncoder(connAlice).Encode(writeOpAlice); err != nil {
		return err
	}

	// We expect two messages: Write Confirmation and Fanout Update.
	// Since Fanout is synchronous and happens before Write Confirmation is sent in the handler,
	// we likely receive Fanout Update first. But we should handle both orders just in case.

	var receivedDataPoint bool
	connAlice.SetReadDeadline(time.Now().Add(2 * time.Second))

	for i := 0; i < 2; i++ {
		resp, err := readTcpResponse(decAlice)
		if err != nil {
			return fmt.Errorf("failed to read response %d: %v", i, err)
		}

		// Check if it's the data point
		var dp DataPoint
		if err := json.Unmarshal(resp.Data, &dp); err == nil && dp.Key != "" {
			if dp.Value == 999 {
				return fmt.Errorf("alice received bob's data (999)")
			}
			if dp.Value == 111 {
				receivedDataPoint = true
			}
		}
	}

	if !receivedDataPoint {
		return fmt.Errorf("alice didn't receive her own data")
	}

	return nil
}

func tcpAuth(conn net.Conn, dec *json.Decoder, token string) error {
	op := Operation{Operation: "auth", Key: token}
	if err := json.NewEncoder(conn).Encode(op); err != nil {
		return err
	}
	resp, err := readTcpResponse(dec)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf(resp.Message)
	}
	return nil
}

func readTcpResponse(dec *json.Decoder) (Response, error) {
	var resp Response
	if err := dec.Decode(&resp); err != nil {
		return Response{}, err
	}
	return resp, nil
}
