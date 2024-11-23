package utils

import (
	"fmt"
	"os"
	"time"
)

var (
	TcpListenAddr  = ":5555"
	HttpListenAddr = ":5556"
	DataDir        = "data"
)

func dateString() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
func InitDataDirectory() {
	//if dataDir does not exist, create it
	if _, err := os.Stat(DataDir); os.IsNotExist(err) {
		err := os.Mkdir(DataDir, 0755)
		if err != nil {
			fmt.Println("Error creating data directory:", err)
			os.Exit(1)
		}
	}
}

func Log(message string, args ...interface{}) {
	fmt.Printf("[%s] 🐹 %s\n", dateString(), fmt.Sprintf(message, args...))
}

func Error(message string, args ...interface{}) {
	fmt.Printf("[%s] 😡 %s\n", dateString(), fmt.Sprintf(message, args...))
}

func Warning(message string, args ...interface{}) {
	fmt.Printf("[%s] 😟 %s\n", dateString(), fmt.Sprintf(message, args...))
}

func Debug(message string, args ...interface{}) {
	fmt.Printf("[%s] 🔍🐹 %s\n", dateString(), fmt.Sprintf(message, args...))
}

func Logln(messages ...interface{}) {
	fmt.Printf("[%s] 🐹 %s\n", dateString(), fmt.Sprint(messages...))
}

func Errorln(messages ...interface{}) {
	fmt.Printf("[%s] 😡 %s\n", dateString(), fmt.Sprint(messages...))
}

func Warningln(messages ...interface{}) {
	fmt.Printf("[%s] 😟 %s\n", dateString(), fmt.Sprint(messages...))
}

func Debugln(messages ...interface{}) {
	fmt.Printf("[%s] 🔍🐹 %s\n", dateString(), fmt.Sprint(messages...))
}

func Panic(v any) {
	fmt.Printf("[%s] 🚨🐹🚨 \n", dateString()) //我被警車包圍了!
	panic(v)
}

func SetupTestFiles() (string, func()) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "gtsdb_test")
	if err != nil {
		//fatal
		fmt.Println("Error creating temporary directory:", err)
		os.Exit(1)
	}

	// Return cleanup function
	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return tmpDir, cleanup
}
