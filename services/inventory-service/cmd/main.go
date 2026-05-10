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

	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/config"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/db"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/handler"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/repository"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/service"
	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
)

func main() {
	cfg := config.Load()

	// ── Database ─────────────────────────────────────────────────
	database, err := db.Connect(cfg)
	if err != nil {
		log.Fatalf("service=inventory db connect: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		log.Fatalf("service=inventory get sql.DB: %v", err)
	}
	defer sqlDB.Close()

	if err := database.AutoMigrate(&model.Inventory{}, &model.InventoryLog{}); err != nil {
		log.Fatalf("service=inventory automigrate: %v", err)
	}

	// ── Seed ─────────────────────────────────────────────────────
	invRepo := repository.NewInventoryRepository(database)
	seeds := []struct {
		productID    uint64
		availableQty int
	}{
		{1001, 100},
		{1002, 50},
	}
	for _, s := range seeds {
		if err := invRepo.SeedIfNotExists(context.Background(), s.productID, s.availableQty); err != nil {
			log.Fatalf("service=inventory seed product %d: %v", s.productID, err)
		}
	}
	log.Println("service=inventory seed complete")

	// ── RabbitMQ ─────────────────────────────────────────────────
	mq, err := messaging.New(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("service=inventory amqp connect: %v", err)
	}
	defer mq.Close()

	if err := messaging.Setup(mq); err != nil {
		log.Fatalf("service=inventory amqp topology: %v", err)
	}

	// ── Wiring ───────────────────────────────────────────────────
	pub := messaging.NewPublisher(mq)
	logRepo := repository.NewInventoryLogRepository(database)
	cmdHandler := service.NewInventoryCommandHandler(database, invRepo, logRepo, pub, cfg.ReserveMaxRetries)

	invSvc := service.NewInventoryService(invRepo)
	h := handler.NewInventoryHandler(invSvc)

	// ── Consumer ─────────────────────────────────────────────────
	consumerCtx, stopConsumers := context.WithCancel(context.Background())
	defer stopConsumers()

	if err := messaging.StartConsumer(consumerCtx, mq, shared.QueueInventoryCommands, cmdHandler.Handle); err != nil {
		log.Fatalf("service=inventory start consumer: %v", err)
	}
	log.Printf("service=inventory consumer started on %s", shared.QueueInventoryCommands)

	// ── HTTP ─────────────────────────────────────────────────────
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "inventory"})
	})
	h.Register(r)

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: r}
	go func() {
		log.Printf("service=inventory port=%s starting", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("service=inventory listen: %v", err)
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
		log.Fatalf("service=inventory forced shutdown: %v", err)
	}
	log.Println("service=inventory exited")
}
