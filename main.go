package main

import (
	"collector/metrics"
	"errors"

	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/tomb.v2"
)

// callerPrettifier simplifies the caller info
func callerPrettifier(f *runtime.Frame) (function string, file string) {
	function = f.Function[strings.LastIndex(f.Function, "/")+1:]
	file = fmt.Sprint(f.File[strings.LastIndex(f.File, "/")+1:], ":", f.Line)
	return function, file
}

// setupSignalHandler setup signal handler for TiDB Server
func setupSignalHandler(shutdownFunc func()) {
	closeSignalChan := make(chan os.Signal, 1)
	signal.Notify(closeSignalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-closeSignalChan
		log.Infof("got signal %s to exit", sig.String())
		shutdownFunc()
	}()
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
	viper.SetConfigFile("config.yml")
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("failed to read config: ", err)
		return
	}

	tom := &tomb.Tomb{}
	setupSignalHandler(func() {
		tom.Kill(errors.New("collector terminates")) // kill other goroutines
	})
	tom.Go(func() error {
		metrics.NewReporter().Start(tom)
		return nil
	})
	select {
	case <-tom.Dying():
		<-tom.Dead() // gracefully wait until all goroutines dead
		return
	}
}
