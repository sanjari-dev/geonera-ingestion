package logger

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

type contextHook struct{}

func (h *contextHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *contextHook) Fire(entry *logrus.Entry) error {
	if _, exists := entry.Data["trace_id"]; !exists {
		traceID := ""
		if entry.Context != nil {
			traceID = TraceIDFromContext(entry.Context)
		}
		entry.Data["trace_id"] = traceID
	}
	if _, exists := entry.Data["state_id"]; !exists {
		stateID := ""
		if entry.Context != nil {
			stateID = StateIDFromContext(entry.Context)
		}
		entry.Data["state_id"] = stateID
	}
	return nil
}

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339})
	logrus.AddHook(&contextHook{})
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
