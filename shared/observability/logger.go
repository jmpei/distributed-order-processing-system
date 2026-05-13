package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"go.uber.org/zap"
)

type ctxKey int

const (
	keyTraceID ctxKey = iota
	keyLogger
	keySagaID
)

// NewLogger returns a zap.Logger pre-bound with service=<name>. Output is
// JSON to stdout, errors to stderr (Production preset).
func NewLogger(service string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.DisableStacktrace = true // saga retries make stacktraces noisy
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	base, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	return base.With(zap.String("service", service)), nil
}

// NewTraceID returns a 32-char hex string suitable for X-Request-ID and the
// AMQP trace_id header. crypto/rand keeps shared/ free of UUID deps.
func NewTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ── Context plumbing ────────────────────────────────────────────────

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, keyTraceID, traceID)
}

func TraceIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(keyTraceID).(string); ok {
		return v
	}
	return ""
}

// WithLogger stores l in ctx; LoggerFrom returns it (or a no-op).
func WithLogger(ctx context.Context, l *zap.Logger) context.Context {
	return context.WithValue(ctx, keyLogger, l)
}

func LoggerFrom(ctx context.Context) *zap.Logger {
	if v, ok := ctx.Value(keyLogger).(*zap.Logger); ok && v != nil {
		return v
	}
	return zap.NewNop()
}

// WithSagaID stores saga_id in ctx and rebinds the ctx logger to include it.
// Callers downstream can call LoggerFrom(ctx).Info(...) and get saga_id for free.
func WithSagaID(ctx context.Context, sagaID string) context.Context {
	ctx = context.WithValue(ctx, keySagaID, sagaID)
	if l, ok := ctx.Value(keyLogger).(*zap.Logger); ok && l != nil {
		ctx = WithLogger(ctx, l.With(zap.String("saga_id", sagaID)))
	}
	return ctx
}

func SagaIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(keySagaID).(string); ok {
		return v
	}
	return ""
}
