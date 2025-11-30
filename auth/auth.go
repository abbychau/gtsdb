package auth

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"gtsdb/utils"
	"os"
	"sync"
)

type User struct {
	Name  string `json:"name"`
	Token string `json:"token"`
}

var (
	users      = make(map[string]User)
	usersMutex sync.RWMutex
	usersFile  string
)

func Init(dataDir string) {
	usersFile = dataDir + "/users.json"
	loadUsers()

	if utils.RootToken != "" {
		users["root"] = User{Name: "root", Token: utils.RootToken}
		saveUsers()
		utils.Logln("Root user token set from config")
	} else if _, ok := users["root"]; !ok {
		// Create default root user if not exists
		token, _ := generateToken()
		users["root"] = User{Name: "root", Token: token}
		saveUsers()
		utils.Logln("Created default root user with token:", token)
	}
}

func loadUsers() {
	usersMutex.Lock()
	defer usersMutex.Unlock()

	if _, err := os.Stat(usersFile); os.IsNotExist(err) {
		return
	}

	data, err := os.ReadFile(usersFile)
	if err != nil {
		utils.Errorln("Error reading users file:", err)
		return
	}

	var userList []User
	if err := json.Unmarshal(data, &userList); err != nil {
		utils.Errorln("Error parsing users file:", err)
		return
	}

	for _, u := range userList {
		users[u.Name] = u
	}
}

func saveUsers() {
	var userList []User
	for _, u := range users {
		userList = append(userList, u)
	}

	data, err := json.MarshalIndent(userList, "", "  ")
	if err != nil {
		utils.Errorln("Error marshalling users:", err)
		return
	}

	if err := os.WriteFile(usersFile, data, 0644); err != nil {
		utils.Errorln("Error writing users file:", err)
	}
}

func generateToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func CreateUser(name string) (User, error) {
	usersMutex.Lock()
	defer usersMutex.Unlock()

	if name == "" {
		// Generate a unique name if empty
		for {
			token, _ := generateToken()
			name = "user_" + token[:8]
			if _, exists := users[name]; !exists {
				break
			}
		}
	}

	if _, exists := users[name]; exists {
		return User{}, errors.New("user already exists")
	}

	token, err := generateToken()
	if err != nil {
		return User{}, err
	}

	user := User{Name: name, Token: token}
	users[name] = user
	saveUsers()
	return user, nil
}

func ResetUserToken(name string) (string, error) {
	usersMutex.Lock()
	defer usersMutex.Unlock()

	user, exists := users[name]
	if !exists {
		return "", errors.New("user not found")
	}

	token, err := generateToken()
	if err != nil {
		return "", err
	}

	user.Token = token
	users[name] = user
	saveUsers()
	return user.Token, nil
}

func VerifyToken(token string) (User, bool) {
	usersMutex.RLock()
	defer usersMutex.RUnlock()

	for _, u := range users {
		if u.Token == token {
			return u, true
		}
	}
	return User{}, false
}

func GetUser(name string) (User, bool) {
	usersMutex.RLock()
	defer usersMutex.RUnlock()
	u, ok := users[name]
	return u, ok
}
