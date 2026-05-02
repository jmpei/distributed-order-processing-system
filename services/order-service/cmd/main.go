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

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/config"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/db"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/handler"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/repository"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/service"
	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
)

func main() {
	cfg := config.Load()

	// ── Database ─────────────────────────────────────────────────
	database, err := db.Connect(cfg)
	if err != nil {
		log.Fatalf("service=order db connect: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		log.Fatalf("service=order get sql.DB: %v", err)
	}
	defer sqlDB.Close()

	if err := database.AutoMigrate(&model.Order{}, &model.SagaState{}, &model.ProcessedEvent{}); err != nil {
		log.Fatalf("service=order automigrate: %v", err)
	}

	// ── RabbitMQ ─────────────────────────────────────────────────
	mq, err := messaging.New(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("service=order amqp connect: %v", err)
	}
	defer mq.Close()

	if err := messaging.Setup(mq); err != nil {
		log.Fatalf("service=order amqp topology: %v", err)
	}

	// ── Wiring ───────────────────────────────────────────────────
	pub := messaging.NewPublisher(mq)

	orderRepo := repository.NewOrderRepository(database)
	sagaRepo := repository.NewSagaRepository(database)
	orderSvc := service.NewOrderService(orderRepo)
	orchestrator := service.NewSagaOrchestrator(database, sagaRepo, orderRepo, pub)

	h := handler.NewOrderHandler(orderSvc, orchestrator)

	// ── Consumer ─────────────────────────────────────────────────
	consumerCtx, stopConsumers := context.WithCancel(context.Background())
	defer stopConsumers()

	if err := messaging.StartConsumer(consumerCtx, mq, shared.QueueOrderEvents, orchestrator.HandleEvent); err != nil {
		log.Fatalf("service=order start consumer: %v", err)
	}
	log.Printf("service=order consumer started on %s", shared.QueueOrderEvents)

	// ── HTTP ─────────────────────────────────────────────────────
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "order"})
	})
	h.Register(r)

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: r}
	go func() {
		log.Printf("service=order port=%s starting", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("service=order listen: %v", err)
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
		log.Fatalf("service=order forced shutdown: %v", err)
	}
	log.Println("service=order exited")
}
