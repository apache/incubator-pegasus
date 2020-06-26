package main

import (
	"fmt"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

// callerPrettifier simplifies the caller info
func callerPrettifier(f *runtime.Frame) (function string, file string) {
	function = f.Function[strings.LastIndex(f.Function, "/")+1:]
	file = fmt.Sprint(f.File[strings.LastIndex(f.File, "/")+1:], ":", f.Line)
	return function, file
}

func main() {
	// initialize logging
	log.SetFormatter(&log.TextFormatter{
		DisableColors:    true,
		FullTimestamp:    true,
		CallerPrettyfier: callerPrettifier,
	})
	log.SetOutput(&lumberjack.Logger{
		Filename:  "./pegasus.log",
		MaxSize:   500, // MegaBytes
		MaxAge:    5,   // days
		LocalTime: true,
	})
	log.SetReportCaller(true)

	// TODO(wutao1): use args[1] as config path
	viper.AddConfigPath("config.yaml")
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("failed to read config: ", err)
		return
	}
}
