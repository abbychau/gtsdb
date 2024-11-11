package main

import (
	"fmt"
	"gtsdb/buffer"
	"gtsdb/fanout"
	"gtsdb/utils"
	"net"
	"os"
)

var fanoutManager = fanout.NewFanout()

func main() {
	utils.InitDataDirectory()
	fanoutManager.Start()                //this will start 2 go routines in the background
	go buffer.StartPeriodicFlushWorker() //this will start 1 go routine in the background

	defer func() {
		// This will run when the main function exits
		fmt.Println("GTSDB is shutting down...")
		buffer.FlushRemainingDataPoints()
		fmt.Println("Done.")
	}()

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
