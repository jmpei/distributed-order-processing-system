package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/config"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/db"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/handler"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/repository"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/service"
	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
	"github.com/TomatoesSuck/distributed-order-processing/shared/observability"
)

const serviceName = "payment"

func main() {
	logger, err := observability.NewLogger(serviceName)
	if err != nil {
		panic("zap init: " + err.Error())
	}
	defer logger.Sync() //nolint:errcheck

	cfg := config.Load()

	// ── Database ─────────────────────────────────────────────────
	database, err := db.Connect(cfg)
	if err != nil {
		logger.Fatal("db connect", zap.Error(err))
	}
	sqlDB, err := database.DB()
	if err != nil {
		logger.Fatal("get sql.DB", zap.Error(err))
	}
	defer sqlDB.Close()

	if err := database.AutoMigrate(&model.Payment{}, &model.ProcessedEvent{}); err != nil {
		logger.Fatal("automigrate", zap.Error(err))
	}

	// ── RabbitMQ ─────────────────────────────────────────────────
	mq, err := messaging.New(cfg.RabbitMQURL)
	if err != nil {
		logger.Fatal("amqp connect", zap.Error(err))
	}
	defer mq.Close()

	if err := messaging.Setup(mq); err != nil {
		logger.Fatal("amqp topology", zap.Error(err))
	}

	// ── Wiring ───────────────────────────────────────────────────
	pub := messaging.NewPublisher(mq, logger)
	paymentRepo := repository.NewPaymentRepository(database)
	eventRepo := repository.NewProcessedEventRepository(database)
	cmdHandler := service.NewPaymentCommandHandler(paymentRepo, eventRepo, pub, cfg.PaymentFailureRate)

	paymentSvc := service.NewPaymentService(paymentRepo)
	h := handler.NewPaymentHandler(paymentSvc)

	// ── Consumer ─────────────────────────────────────────────────
	consumerCtx, stopConsumers := context.WithCancel(context.Background())
	defer stopConsumers()

	if err := messaging.StartConsumer(consumerCtx, mq, shared.QueuePaymentCommands, logger, cmdHandler.Handle); err != nil {
		logger.Fatal("start consumer", zap.Error(err))
	}
	logger.Info("consumer started", zap.String("queue", shared.QueuePaymentCommands))

	// ── HTTP ─────────────────────────────────────────────────────
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(observability.GinMiddleware(serviceName, logger))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": serviceName})
	})
	h.Register(r)

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: r}
	go func() {
		logger.Info("http listening", zap.String("port", cfg.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("listen", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	stopConsumers()

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Fatal("forced shutdown", zap.Error(err))
	}
	logger.Info("exited")
}
