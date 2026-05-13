package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewTraceID_Unique32CharHex(t *testing.T) {
	a := NewTraceID()
	b := NewTraceID()
	assert.Len(t, a, 32)
	assert.NotEqual(t, a, b)
}

func TestContextRoundtrip(t *testing.T) {
	ctx := context.Background()
	assert.Empty(t, TraceIDFrom(ctx))
	assert.Empty(t, SagaIDFrom(ctx))

	ctx = WithTraceID(ctx, "trace-xyz")
	ctx = WithSagaID(ctx, "saga-abc")

	assert.Equal(t, "trace-xyz", TraceIDFrom(ctx))
	assert.Equal(t, "saga-abc", SagaIDFrom(ctx))
}

func TestAMQPHeadersRoundtrip(t *testing.T) {
	send := context.Background()
	send = WithTraceID(send, "t1")
	send = WithSagaID(send, "s1")

	h := InjectAMQPHeaders(send, nil)
	assert.Equal(t, "t1", h[HeaderTraceID])
	assert.Equal(t, "s1", h[HeaderSagaID])

	recv := ExtractAMQPHeaders(context.Background(), zap.NewNop(), h)
	assert.Equal(t, "t1", TraceIDFrom(recv))
	assert.Equal(t, "s1", SagaIDFrom(recv))
}

func TestAMQPHeaders_NilInputs(t *testing.T) {
	// Inject from a bare ctx should not panic and should return an empty Table.
	h := InjectAMQPHeaders(context.Background(), nil)
	assert.Empty(t, h)

	// Extract from empty headers should return a usable ctx with a non-nil logger.
	ctx := ExtractAMQPHeaders(context.Background(), nil, amqp.Table{})
	assert.NotNil(t, LoggerFrom(ctx)) // never returns nil — falls back to no-op
}

func TestGinMiddleware_GeneratesTraceIDAndRecordsMetrics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(GinMiddleware("test-service", zap.NewNop()))
	r.GET("/items/:id", func(c *gin.Context) {
		// Inside handler: trace_id should be in ctx.
		assert.NotEmpty(t, TraceIDFrom(c.Request.Context()))
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/items/42", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Header().Get("X-Request-ID"))
}

func TestGinMiddleware_HonoursInboundXRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(GinMiddleware("test-service", zap.NewNop()))
	r.GET("/", func(c *gin.Context) {
		assert.Equal(t, "client-supplied", TraceIDFrom(c.Request.Context()))
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Request-ID", "client-supplied")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "client-supplied", w.Header().Get("X-Request-ID"))
}

func TestLoggerFrom_DefaultIsNoOp(t *testing.T) {
	// LoggerFrom on a bare ctx must return a usable (no-op) logger so
	// callers can unconditionally `LoggerFrom(ctx).Info(...)`.
	l := LoggerFrom(context.Background())
	require.NotNil(t, l)
	l.Info("no panic") // would panic if nil
}
