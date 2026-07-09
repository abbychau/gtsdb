package main

import (
	"context"
	"errors"
	"flag"
	"gtsdb/auth"
	"gtsdb/buffer"
	"gtsdb/fanout"
	"gtsdb/handlers"
	"gtsdb/utils"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/ini.v1"
)

func main() {
	// Set default config file
	defaultConfig := "gtsdb.ini"

	// Parse command line arguments
	flag.Parse()
	configFile := defaultConfig
	if args := flag.Args(); len(args) > 0 {
		configFile = args[0]
	}

	run(configFile)
}

func run(configFile string) {
	loadConfig(configFile)
	utils.InitDataDirectory()
	migrateData()
	auth.Init(utils.DataDir)
	fanoutManager := fanout.NewFanout(100000) // Buffer size of 1000 for production use

	// Create stop channels
	tcpStop := make(chan struct{})
	httpStop := make(chan struct{})

	go startTCPServerWithStop(fanoutManager, tcpStop)
	go startHTTPServerWithStop(fanoutManager, httpStop)

	// Start background compaction (checks every hour, compacts files > 100MB)
	compactStop := startBackgroundCompaction(1*time.Hour, 100*1024*1024)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// Stop servers
	close(tcpStop)
	close(httpStop)
	close(compactStop)
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
			if errors.Is(err, net.ErrClosed) {
				return
			}
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		utils.Errorln("HTTP server error:", err)
	}
}

func loadConfig(iniFile string) {
	utils.Logln("歡迎使用🐹小倉鼠🐹時序資料庫 🐁🐁 ")
	utils.Logln("🎶吱吱🎶吱吱🎶 🐹")
	utils.Log("🏃現在在用 %v 唷", iniFile)
	utils.Log("今天是：%s 哦", time.Now().Format("2006-01-02 15:04:05"))

	cfg, err := ini.InsensitiveLoad(iniFile)
	if err != nil {
		utils.Warningln("無法讀取配置文件：", err)
	} else {
		utils.TcpListenAddr = cfg.Section("listens").Key("tcp").String()
		utils.HttpListenAddr = cfg.Section("listens").Key("http").String()
		utils.DataDir = cfg.Section("paths").Key("data").String()
		utils.NoAuthUser = cfg.Section("auth").Key("no_auth_user").String()
		utils.RootToken = cfg.Section("auth").Key("root_token").String()

		// Load file handle LRU capacity (optional, defaults to 700)
		if capacityStr := cfg.Section("buffer").Key("file_handle_lru_capacity").String(); capacityStr != "" {
			if capacity := cfg.Section("buffer").Key("file_handle_lru_capacity").MustInt(700); capacity > 0 {
				utils.FileHandleLRUCapacity = capacity
			}
		}
	}

	utils.Logln(" TCP 監聽地址： ", utils.TcpListenAddr)
	utils.Logln("HTTP 監聽地址： ", utils.HttpListenAddr)
	utils.Logln(" 數據存儲目錄： ", utils.DataDir)
	utils.Logln("文件句柄LRU容量： ", utils.FileHandleLRUCapacity)

	buffer.InitFileHandles()
	buffer.InitIDSet()

	utils.Log("📊 我們現在有 %d 組時序", len(buffer.GetAllIds()))
}

func gracefulShutdown() {
	utils.Logln("中斷信號來了！小倉鼠要先把所有數據存好...吱吱")
	buffer.FlushRemainingDataPoints()
	utils.Logln("安全放好食物回家了啦！拜拜！下次來玩喔！")
}

func migrateData() {
	// Check if there are any .aof or .idx files in the root of DataDir
	files, err := os.ReadDir(utils.DataDir)
	if err != nil {
		utils.Errorln("Error reading data directory for migration:", err)
		return
	}

	foundFiles := false
	for _, file := range files {
		if !file.IsDir() && (strings.HasSuffix(file.Name(), ".aof") || strings.HasSuffix(file.Name(), ".idx")) {
			foundFiles = true
			break
		}
	}

	if !foundFiles {
		return
	}

	targetUser := utils.NoAuthUser
	if targetUser == "" {
		targetUser = "root"
	}

	targetDir := filepath.Join(utils.DataDir, targetUser)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			utils.Errorln("Error creating migration directory:", err)
			return
		}
	}

	utils.Logln("Migrating existing data to user:", targetUser)

	for _, file := range files {
		if !file.IsDir() && (strings.HasSuffix(file.Name(), ".aof") || strings.HasSuffix(file.Name(), ".idx")) {
			oldPath := filepath.Join(utils.DataDir, file.Name())
			newPath := filepath.Join(targetDir, file.Name())
			if err := os.Rename(oldPath, newPath); err != nil {
				utils.Errorln("Error moving file:", file.Name(), err)
			}
		}
	}
}

// startBackgroundCompaction runs periodic WAL compaction in the background.
// It checks all keys and compacts files that exceed the threshold size.
func startBackgroundCompaction(interval time.Duration, thresholdBytes int64) chan struct{} {
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				utils.Log("Starting background compaction check (threshold: %d bytes)", thresholdBytes)
				ids := buffer.GetAllIds()
				for _, id := range ids {
					select {
					case <-stop:
						return
					default:
					}
					fh, ok := buffer.GetDataFileHandle(id + ".aof")
					if !ok || fh == nil {
						continue
					}
					stat, err := fh.Stat()
					if err != nil {
						continue
					}
					if stat.Size() > thresholdBytes {
						utils.Log("Auto-compacting key %s (size: %d bytes)", id, stat.Size())
						if err := buffer.CompactKey(id); err != nil {
							utils.Error("Auto-compaction failed for %s: %v", id, err)
						}
					}
				}
			}
		}
	}()
	return stop
}
