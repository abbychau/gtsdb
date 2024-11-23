package main

import (
	"gtsdb/buffer"
	"gtsdb/fanout"
	"gtsdb/handlers"
	"gtsdb/utils"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"gopkg.in/ini.v1"
)

func main() {
	loadConfig("gtsdb.ini")
	utils.InitDataDirectory()
	fanoutManager := fanout.NewFanout()
	go startTCPServer(fanoutManager)
	go startHTTPServer(fanoutManager)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	gracefulShutdown()
}

func startTCPServer(fanoutManager *fanout.Fanout) {
	listener, err := net.Listen("tcp", utils.TcpListenAddr)
	if err != nil {
		utils.Errorln("Error listening:", err)
		os.Exit(1)
	}
	defer listener.Close()

	utils.Logln("👂 用心監聽 TCP " + utils.TcpListenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			utils.Errorln("Error accepting connection:", err)
			continue
		}
		go handlers.HandleTcpConnection(conn, fanoutManager)
	}
}

func startHTTPServer(fanoutManager *fanout.Fanout) {
	utils.Log("👂 用心監聽 HTTP " + utils.HttpListenAddr)
	http.ListenAndServe(utils.HttpListenAddr, handlers.SetupHTTPRoutes(fanoutManager))
}

func loadConfig(iniFile string) {
	utils.Logln("歡迎使用🐹小倉鼠🐹時序資料庫 🐁🐁 ")
	utils.Logln("🎶吱吱🎶吱吱🎶 🐹")
	utils.Logln("🏃跑🏃跑跑跑🏃 🐹")
	utils.Log("今天是：%s 哦", time.Now().Format("2006-01-02 15:04:05"))

	cfg, err := ini.Load(iniFile)
	if err != nil {
		utils.Warningln("無法讀取配置文件：", err)
	} else {
		utils.TcpListenAddr = cfg.Section("listens").Key("tcp").String()
		utils.HttpListenAddr = cfg.Section("listens").Key("http").String()
		utils.DataDir = cfg.Section("paths").Key("data").String()
	}

	utils.Logln("TCP 監聽地址：", utils.TcpListenAddr)
	utils.Logln("HTTP 監聽地址：", utils.HttpListenAddr)
	utils.Logln("數據存儲目錄：", utils.DataDir)
}

func gracefulShutdown() {
	utils.Logln("中斷信號來了！小倉鼠要先把所有數據存好...吱吱")
	buffer.FlushRemainingDataPoints()
	utils.Logln("安全放好食物回家了啦！拜拜！下次來玩喔！")
}
