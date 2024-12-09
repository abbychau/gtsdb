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
	run("gtsdb.ini")
}

func run(configFile string) {
	loadConfig(configFile)
	utils.InitDataDirectory()
	fanoutManager := fanout.NewFanout()

	// Create stop channels
	tcpStop := make(chan struct{})
	httpStop := make(chan struct{})

	go startTCPServerWithStop(fanoutManager, tcpStop)
	go startHTTPServerWithStop(fanoutManager, httpStop)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// Stop servers
	close(tcpStop)
	close(httpStop)
	gracefulShutdown()
}

func startTCPServerWithStop(fanoutManager *fanout.Fanout, stop chan struct{}) {
	listener, err := net.Listen("tcp", utils.TcpListenAddr)
	if err != nil {
		utils.Errorln("Error listening:", err)
		return
	}
	defer listener.Close()

	go func() {
		<-stop
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-stop:
				return
			default:
				utils.Errorln("Error accepting connection:", err)
				continue
			}
		}
		go handlers.HandleTcpConnection(conn, fanoutManager)
	}
}

func startHTTPServerWithStop(fanoutManager *fanout.Fanout, stop chan struct{}) {
	srv := &http.Server{
		Addr:    utils.HttpListenAddr,
		Handler: handlers.SetupHTTPRoutes(fanoutManager),
	}

	go func() {
		<-stop
		srv.Close()
	}()

	srv.ListenAndServe()
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

	utils.Logln(" TCP 監聽地址： ", utils.TcpListenAddr)
	utils.Logln("HTTP 監聽地址： ", utils.HttpListenAddr)
	utils.Logln(" 數據存儲目錄： ", utils.DataDir)

	buffer.InitIDSet()

	utils.Log("📊 我們現在有 %d 組時序", len(buffer.GetAllIds()))
}

func gracefulShutdown() {
	utils.Logln("中斷信號來了！小倉鼠要先把所有數據存好...吱吱")
	buffer.FlushRemainingDataPoints()
	utils.Logln("安全放好食物回家了啦！拜拜！下次來玩喔！")
}
