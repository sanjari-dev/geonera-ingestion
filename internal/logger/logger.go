package logger

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339})
	level := logrus.TraceLevel
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		if parsed, err := logrus.ParseLevel(v); err == nil {
			level = parsed
		} else {
			logrus.WithFields(logrus.Fields{"value": v, "valid": "trace debug info warn error fatal panic"}).Warn("logger: invalid LOG_LEVEL")
		}
	}
	logrus.SetLevel(level)
}
