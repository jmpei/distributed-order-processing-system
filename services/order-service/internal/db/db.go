package db

import (
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/config"
)

func Connect(cfg config.Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=UTC",
		cfg.DBUser, cfg.DBPass, cfg.DBHost, cfg.DBPort, cfg.DBName)

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err == nil {
			sqlDB, e := db.DB()
			if e != nil {
				return nil, fmt.Errorf("get sql.DB: %w", e)
			}
			sqlDB.SetMaxOpenConns(cfg.DBMaxOpenConns)
			sqlDB.SetMaxIdleConns(cfg.DBMaxIdleConns)
			sqlDB.SetConnMaxLifetime(time.Duration(cfg.DBConnMaxLifetimeMin) * time.Minute)
			// Expose go_sql_stats_* (in_use, idle, wait_count, wait_duration, …)
			// so Prometheus can show pool saturation during load tests.
			prometheus.DefaultRegisterer.MustRegister(
				collectors.NewDBStatsCollector(sqlDB, "order"),
			)
			return db, nil
		}
		lastErr = err
		log.Printf("service=order db connect failed, retrying in 2s: %v", err)
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("db connect timeout after 30s: %w", lastErr)
}
