package observability

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// GinMiddleware installs request-scoped trace_id + logger into ctx,
// records http_requests_total + http_request_duration_seconds, and emits
// one structured log line per request.
//
// Skips /metrics so Prometheus scrapes don't spam logs or inflate counters.
func GinMiddleware(service string, logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/metrics" {
			c.Next()
			return
		}

		// Trace id: honour incoming X-Request-ID if present (lets ALB /
		// upstream caller stitch traces), else mint one.
		traceID := c.GetHeader("X-Request-ID")
		if traceID == "" {
			traceID = NewTraceID()
		}
		c.Writer.Header().Set("X-Request-ID", traceID)

		reqLogger := logger.With(zap.String("trace_id", traceID))

		ctx := c.Request.Context()
		ctx = WithTraceID(ctx, traceID)
		ctx = WithLogger(ctx, reqLogger)
		c.Request = c.Request.WithContext(ctx)

		start := time.Now()
		c.Next()
		dur := time.Since(start)

		// Use route pattern (e.g. "/orders/:id") not raw URL so the
		// label cardinality stays bounded.
		path := c.FullPath()
		if path == "" {
			path = "unmatched"
		}
		status := c.Writer.Status()

		HTTPRequestsTotal.
			WithLabelValues(service, c.Request.Method, path, strconv.Itoa(status)).
			Inc()
		HTTPRequestDuration.
			WithLabelValues(service, c.Request.Method, path).
			Observe(dur.Seconds())

		reqLogger.Info("http_request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.String("route", path),
			zap.Int("status", status),
			zap.Duration("duration", dur),
		)
	}
}
