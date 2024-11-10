package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	fanoutManager.Start() //this will start 2 go routines in the background
	defer func() {
		// This will run when the main function exits
		fmt.Println("GTSDB is shutting down...")
		flushRemainingDataPoints()
		fmt.Println("Done.")
	}()

	//if dataDir does not exist, create it
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		err := os.Mkdir(dataDir, 0755)
		if err != nil {
			fmt.Println("Error creating data directory:", err)
			os.Exit(1)
		}
	}
	listener, err := net.Listen("tcp", ":5555")
	if err != nil {
		fmt.Println("Error listening:", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Listening on :5555")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}
