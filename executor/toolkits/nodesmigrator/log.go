package nodesmigrator

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

func logInfo(log string) {
	fmt.Printf("INFO: %s\n", log)
	logrus.Info(log)
}

func logWarn(log string) {
	fmt.Printf("WARN: %s\n", log)
	logrus.Warn(log)
}

func logDebug(log string) {
	logrus.Debugf(log)
}

func logPanic(log string) {
	fmt.Println(log)
	logrus.Panic(log)
}
