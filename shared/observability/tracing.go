package observability

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// AMQP header names. Keep stable: they are part of the inter-service
// contract just like the message body schema.
const (
	HeaderTraceID = "trace_id"
	HeaderSagaID  = "saga_id"
)

// InjectAMQPHeaders copies trace_id and saga_id from ctx into h. Used by
// publishers so the consumer on the other end can reconstruct the ctx.
// If h is nil a fresh amqp.Table is returned.
func InjectAMQPHeaders(ctx context.Context, h amqp.Table) amqp.Table {
	if h == nil {
		h = amqp.Table{}
	}
	if tid := TraceIDFrom(ctx); tid != "" {
		h[HeaderTraceID] = tid
	}
	if sid := SagaIDFrom(ctx); sid != "" {
		h[HeaderSagaID] = sid
	}
	return h
}

// ExtractAMQPHeaders is the consumer-side mirror: reads trace_id/saga_id
// out of headers and seeds them into ctx (and the ctx logger). Pass it
// the per-consumer base logger so the returned ctx carries a logger
// pre-bound with service + trace_id + saga_id.
func ExtractAMQPHeaders(ctx context.Context, base *zap.Logger, h amqp.Table) context.Context {
	traceID, _ := h[HeaderTraceID].(string)
	sagaID, _ := h[HeaderSagaID].(string)

	if traceID != "" {
		ctx = WithTraceID(ctx, traceID)
	}

	l := base
	if l == nil {
		l = zap.NewNop()
	}
	if traceID != "" {
		l = l.With(zap.String("trace_id", traceID))
	}
	ctx = WithLogger(ctx, l)

	if sagaID != "" {
		ctx = WithSagaID(ctx, sagaID)
	}

	return ctx
}
