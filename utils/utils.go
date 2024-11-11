package utils

import (
	"fmt"
	"os"
)

var DataDir = "data"

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
