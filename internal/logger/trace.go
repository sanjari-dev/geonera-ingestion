package logger

import (
	"context"

	"github.com/sirupsen/logrus"
)

type traceKey struct{}

// WithTraceID stores a traceId into the context. Call this once per incoming
// request (MQ message or HTTP request) so every downstream operation carries
// the same identifier for correlation in the log aggregator.
func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceKey{}, id)
}

// TraceIDFromContext retrieves the traceId stored by WithTraceID.
// Returns an empty string if no traceId has been set.
func TraceIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(traceKey{}).(string)
	return id
}

// T returns a logrus Entry pre-populated with the trace_id from ctx.
// All Trace-level calls should go through T(ctx).Trace(...) so every log
// line is automatically correlated to its originating request.
//
// Usage:
//
//	logger.T(ctx).WithField("fn", "executeIngestionETL").Trace("fn_entry")
//	logger.T(ctx).WithFields(logrus.Fields{"query": "State.First", "err": err}).Trace("after query")
func T(ctx context.Context) *logrus.Entry {
	return logrus.WithField("trace_id", TraceIDFromContext(ctx))
}
