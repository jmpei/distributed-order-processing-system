package config

import "os"

type Config struct {
	Port        string
	DBHost      string
	DBPort      string
	DBUser      string
	DBPass      string
	DBName      string
	RabbitMQURL string
}

func Load() Config {
	return Config{
		Port:        getEnv("INVENTORY_PORT", "8082"),
		DBHost:      getEnv("DB_HOST", "localhost"),
		DBPort:      getEnv("DB_PORT", "3306"),
		DBUser:      getEnv("DB_USER", "root"),
		DBPass:      getEnv("DB_PASS", "rootpw"),
		DBName:      getEnv("DB_NAME", "inventory_db"),
		RabbitMQURL: getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
