package logger

import (
	"time"

	"github.com/sirupsen/logrus"
)

// L is the global structured logger. All packages use the logrus global
// functions (logrus.Infof, logrus.Errorf, logrus.Fatalf, etc.) which delegate here.
var L = logrus.StandardLogger()

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})
	logrus.SetLevel(logrus.InfoLevel)
}
