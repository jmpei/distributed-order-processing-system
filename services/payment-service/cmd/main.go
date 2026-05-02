package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/config"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/db"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/handler"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/repository"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/service"
	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
)

func main() {
	cfg := config.Load()

	// ── Database ─────────────────────────────────────────────────
	database, err := db.Connect(cfg)
	if err != nil {
		log.Fatalf("service=payment db connect: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		log.Fatalf("service=payment get sql.DB: %v", err)
	}
	defer sqlDB.Close()

	if err := database.AutoMigrate(&model.Payment{}, &model.ProcessedEvent{}); err != nil {
		log.Fatalf("service=payment automigrate: %v", err)
	}

	// ── RabbitMQ ─────────────────────────────────────────────────
	mq, err := messaging.New(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("service=payment amqp connect: %v", err)
	}
	defer mq.Close()

	if err := messaging.Setup(mq); err != nil {
		log.Fatalf("service=payment amqp topology: %v", err)
	}

	// ── Wiring ───────────────────────────────────────────────────
	pub := messaging.NewPublisher(mq)
	paymentRepo := repository.NewPaymentRepository(database)
	eventRepo := repository.NewProcessedEventRepository(database)
	cmdHandler := service.NewPaymentCommandHandler(database, paymentRepo, eventRepo, pub)

	paymentSvc := service.NewPaymentService(paymentRepo)
	h := handler.NewPaymentHandler(paymentSvc)

	// ── Consumer ─────────────────────────────────────────────────
	consumerCtx, stopConsumers := context.WithCancel(context.Background())
	defer stopConsumers()

	if err := messaging.StartConsumer(consumerCtx, mq, shared.QueuePaymentCommands, cmdHandler.Handle); err != nil {
		log.Fatalf("service=payment start consumer: %v", err)
	}
	log.Printf("service=payment consumer started on %s", shared.QueuePaymentCommands)

	// ── HTTP ─────────────────────────────────────────────────────
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "payment"})
	})
	h.Register(r)

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: r}
	go func() {
		log.Printf("service=payment port=%s starting", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("service=payment listen: %v", err)
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
		log.Fatalf("service=payment forced shutdown: %v", err)
	}
	log.Println("service=payment exited")
}
