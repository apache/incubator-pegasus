package nodesmigrator

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

func logInfo(log string, stdout bool) {
	if stdout {
		fmt.Println(log)
	}
	logrus.Info(log)
}

func logWarn(log string, stdout bool) {
	if stdout {
		fmt.Println(log)
	}
	logrus.Warn(log)
}

func logPanic(log string) {
	fmt.Println(log)
	logrus.Panic(log)
}
