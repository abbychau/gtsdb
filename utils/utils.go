package utils

import (
	"fmt"
	"os"
	"time"
)

func dateString() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
func InitDataDirectory(dir string) {
	//if dataDir does not exist, create it
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, 0755)
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
