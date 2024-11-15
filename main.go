package main

import (
	"fmt"
	"gtsdb/buffer"
	"gtsdb/fanout"
	"gtsdb/utils"
	"net"
	"os"
	"os/signal"
	"time"
)

var fanoutManager = fanout.NewFanout()

const listenAddr = ":5555"

func main() {
	utils.Log("歡迎使用🐹小倉鼠🐹時序資料庫 🐁🐁 ")
	utils.Log("🎶吱吱🎶吱吱🎶 🐹")
	utils.Log("🏃跑🏃跑跑跑🏃 🐹")
	utils.Log("今天是：%s 哦", time.Now().Format("2006-01-02 15:04:05"))

	utils.InitDataDirectory()
	fanoutManager.Start() //this will start 2 go routines in the background

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		// graceful shutdown callback
		for range c {
			utils.Log("中斷信號來了！小倉鼠要先把所有數據存好...吱吱")
			buffer.FlushRemainingDataPoints()
			utils.Log("安全放好食物回家了啦！拜拜！下次來玩喔！")
			os.Exit(0)
		}
	}()

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Println("Error listening:", err)
		os.Exit(1)
	}
	defer listener.Close()

	utils.Log("👂 用心監聽 " + listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}
