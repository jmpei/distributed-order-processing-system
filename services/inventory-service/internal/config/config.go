package config

import (
	"log"
	"os"
	"strconv"
)

type Config struct {
	Port              string
	DBHost            string
	DBPort            string
	DBUser            string
	DBPass            string
	DBName            string
	RabbitMQURL       string
	ReserveMaxRetries int
}

func Load() Config {
	return Config{
		Port:              getEnv("INVENTORY_PORT", "8082"),
		DBHost:            getEnv("DB_HOST", "localhost"),
		DBPort:            getEnv("DB_PORT", "3306"),
		DBUser:            getEnv("DB_USER", "root"),
		DBPass:            getEnv("DB_PASS", "rootpw"),
		DBName:            getEnv("DB_NAME", "inventory_db"),
		RabbitMQURL:       getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		ReserveMaxRetries: getEnvInt("INVENTORY_RESERVE_MAX_RETRIES", 50),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		log.Printf("config: invalid %s=%q, using default %d: %v", key, v, fallback, err)
		return fallback
	}
	return n
}
