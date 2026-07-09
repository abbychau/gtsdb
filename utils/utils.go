package utils

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// Log levels
const (
	LogLevelDebug = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

var (
	TcpListenAddr         = ":5555"
	HttpListenAddr        = ":5556"
	DataDir               = "data"
	FileHandleLRUCapacity = 700
	NoAuthUser            = ""
	RootToken             = ""
	CompactionCompression = false
	LogLevel              = int32(LogLevelInfo) // default: info and above
)

// SetLogLevel atomically sets the current log level
func SetLogLevel(level int32) {
	atomic.StoreInt32(&LogLevel, level)
}

func dateString() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

func InitDataDirectory() {
	if _, err := os.Stat(DataDir); os.IsNotExist(err) {
		err := os.Mkdir(DataDir, 0755)
		if err != nil {
			fmt.Println("Error creating data directory:", err)
			os.Exit(1)
		}
	}
}

func Log(message string, args ...interface{}) {
	if atomic.LoadInt32(&LogLevel) <= LogLevelInfo {
		fmt.Printf("[%s] 🐹 %s\n", dateString(), fmt.Sprintf(message, args...))
	}
}

func Error(message string, args ...interface{}) {
	if atomic.LoadInt32(&LogLevel) <= LogLevelError {
		fmt.Printf("[%s] 😡 %s\n", dateString(), fmt.Sprintf(message, args...))
	}
}

func Warning(message string, args ...interface{}) {
	if atomic.LoadInt32(&LogLevel) <= LogLevelWarn {
		fmt.Printf("[%s] 😟 %s\n", dateString(), fmt.Sprintf(message, args...))
	}
}

func Debug(message string, args ...interface{}) {
	if atomic.LoadInt32(&LogLevel) <= LogLevelDebug {
		fmt.Printf("[%s] 🔍🐹 %s\n", dateString(), fmt.Sprintf(message, args...))
	}
}

func Logln(messages ...interface{}) {
	if atomic.LoadInt32(&LogLevel) <= LogLevelInfo {
		fmt.Printf("[%s] 🐹 %s\n", dateString(), fmt.Sprint(messages...))
	}
}

func Errorln(messages ...interface{}) {
	if atomic.LoadInt32(&LogLevel) <= LogLevelError {
		fmt.Printf("[%s] 😡 %s\n", dateString(), fmt.Sprint(messages...))
	}
}

func Warningln(messages ...interface{}) {
	if atomic.LoadInt32(&LogLevel) <= LogLevelWarn {
		fmt.Printf("[%s] 😟 %s\n", dateString(), fmt.Sprint(messages...))
	}
}

func Debugln(messages ...interface{}) {
	if atomic.LoadInt32(&LogLevel) <= LogLevelDebug {
		fmt.Printf("[%s] 🔍🐹 %s\n", dateString(), fmt.Sprint(messages...))
	}
}

func Panic(v any) {
	fmt.Printf("[%s] 🚨🐹🚨 \n", dateString())
	panic(v)
}
