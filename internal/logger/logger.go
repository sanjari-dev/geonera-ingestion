package logger

import (
	"os"
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
	level := logrus.InfoLevel
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		if parsed, err := logrus.ParseLevel(v); err == nil {
			level = parsed
		} else {
			logrus.WithFields(logrus.Fields{"value": v, "valid": "trace debug info warn error fatal panic"}).Warn("logger: invalid LOG_LEVEL")
		}
	}
	logrus.SetLevel(level)
}
