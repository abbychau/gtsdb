package auth

import (
	"encoding/json"
	"gtsdb/utils"
	"os"
	"testing"
)

func setupTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gtsdb-auth-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func cleanupTestDir(t *testing.T, dir string) {
	t.Helper()
	os.RemoveAll(dir)
}

func TestInitAndRootUser(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	// Reset global state
	users = make(map[string]User)

	Init(dir)

	// Verify root user was created
	root, ok := users["root"]
	if !ok {
		t.Fatal("Expected root user to be created")
	}
	if root.Name != "root" {
		t.Errorf("Expected name 'root', got %s", root.Name)
	}
	if len(root.Token) != 32 {
		t.Errorf("Expected token length 32, got %d", len(root.Token))
	}

	// Verify users.json was created
	if _, err := os.Stat(dir + "/users.json"); os.IsNotExist(err) {
		t.Error("Expected users.json to be created")
	}
}

func TestCreateUser(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	users = make(map[string]User)
	usersFile = dir + "/users.json"

	// Create root first
	Init(dir)

	t.Run("create named user", func(t *testing.T) {
		user, err := CreateUser("alice")
		if err != nil {
			t.Fatalf("CreateUser failed: %v", err)
		}
		if user.Name != "alice" {
			t.Errorf("Expected name 'alice', got %s", user.Name)
		}
		if len(user.Token) != 32 {
			t.Errorf("Expected token length 32, got %d", len(user.Token))
		}
	})

	t.Run("create duplicate user", func(t *testing.T) {
		_, err := CreateUser("alice")
		if err == nil {
			t.Error("Expected error for duplicate user")
		}
	})

	t.Run("create user with auto-generated name", func(t *testing.T) {
		user, err := CreateUser("")
		if err != nil {
			t.Fatalf("CreateUser with empty name failed: %v", err)
		}
		if len(user.Name) < 6 {
			t.Errorf("Expected auto-generated name with reasonable length, got %s", user.Name)
		}
		if len(user.Token) != 32 {
			t.Errorf("Expected token length 32, got %d", len(user.Token))
		}
	})
}

func TestVerifyToken(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	users = make(map[string]User)
	usersFile = dir + "/users.json"
	Init(dir)

	root := users["root"]

	t.Run("valid token", func(t *testing.T) {
		user, ok := VerifyToken(root.Token)
		if !ok {
			t.Error("Expected valid token verification")
		}
		if user.Name != "root" {
			t.Errorf("Expected user 'root', got %s", user.Name)
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		_, ok := VerifyToken("invalid-token")
		if ok {
			t.Error("Expected invalid token to fail verification")
		}
	})

	t.Run("empty token", func(t *testing.T) {
		_, ok := VerifyToken("")
		if ok {
			t.Error("Expected empty token to fail verification")
		}
	})
}

func TestResetUserToken(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	users = make(map[string]User)
	usersFile = dir + "/users.json"
	Init(dir)

	t.Run("reset existing user", func(t *testing.T) {
		// Create a user and get their original token
		user, _ := CreateUser("bob")
		oldToken := user.Token

		// Reset the token
		newToken, err := ResetUserToken("bob")
		if err != nil {
			t.Fatalf("ResetUserToken failed: %v", err)
		}
		if newToken == oldToken {
			t.Error("Expected new token to differ from old token")
		}
		if len(newToken) != 32 {
			t.Errorf("Expected token length 32, got %d", len(newToken))
		}

		// Old token should no longer work
		if _, ok := VerifyToken(oldToken); ok {
			t.Error("Expected old token to be invalid after reset")
		}
		// New token should work
		if _, ok := VerifyToken(newToken); !ok {
			t.Error("Expected new token to be valid")
		}
	})

	t.Run("reset non-existent user", func(t *testing.T) {
		_, err := ResetUserToken("nonexistent")
		if err == nil {
			t.Error("Expected error for non-existent user")
		}
	})
}

func TestGetUser(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	users = make(map[string]User)
	usersFile = dir + "/users.json"
	Init(dir)

	_, _ = CreateUser("charlie")

	t.Run("get existing user", func(t *testing.T) {
		user, ok := GetUser("charlie")
		if !ok {
			t.Error("Expected to find user 'charlie'")
		}
		if user.Name != "charlie" {
			t.Errorf("Expected name 'charlie', got %s", user.Name)
		}
	})

	t.Run("get non-existent user", func(t *testing.T) {
		_, ok := GetUser("nonexistent")
		if ok {
			t.Error("Expected false for non-existent user")
		}
	})
}

func TestRootTokenFromConfig(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	users = make(map[string]User)
	usersFile = dir + "/users.json"

	// Override RootToken and call Init
	origRootToken := utils.RootToken
	utils.RootToken = "my-custom-root-token"
	defer func() { utils.RootToken = origRootToken }()

	Init(dir)

	root, ok := users["root"]
	if !ok {
		t.Fatal("Expected root user to be created")
	}
	if root.Token != "my-custom-root-token" {
		t.Errorf("Expected root token from config, got %s", root.Token)
	}
}

func TestLoadUsersFromFile(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	// Pre-create a users.json with known data (JSON array format)
	usersData := []User{
		{Name: "loaded-user", Token: "loaded-token-123"},
	}
	data, err := json.Marshal(usersData)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/users.json", data, 0644); err != nil {
		t.Fatal(err)
	}

	// Reset and reload
	users = make(map[string]User)
	usersFile = dir + "/users.json"

	// Clear RootToken so Init doesn't overwrite
	origRootToken := utils.RootToken
	utils.RootToken = ""
	defer func() { utils.RootToken = origRootToken }()

	Init(dir)

	// Verify the user was loaded
	user, ok := users["loaded-user"]
	if !ok {
		t.Fatal("Expected 'loaded-user' to be loaded from file")
	}
	if user.Token != "loaded-token-123" {
		t.Errorf("Expected token 'loaded-token-123', got %s", user.Token)
	}
}

func TestSaveUsers(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	users = make(map[string]User)
	usersFile = dir + "/users.json"
	users["test-save"] = User{Name: "test-save", Token: "save-token"}

	saveUsers()

	// Read back the file (JSON array format)
	data, err := os.ReadFile(dir + "/users.json")
	if err != nil {
		t.Fatal(err)
	}
	var loaded []User
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, u := range loaded {
		if u.Name == "test-save" && u.Token == "save-token" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected 'test-save' with token 'save-token' in saved file")
	}
}
