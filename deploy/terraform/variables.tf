variable "region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-west-2"
}

variable "project" {
  description = "Short project name, used as a prefix for resource names"
  type        = string
  default     = "dop"
}

variable "environment" {
  description = "Deployment environment label (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ, ALB lives here)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ, ECS + MQ live here)"
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24"]
}

variable "admin_cidr" {
  description = "CIDR allowed to reach RDS on 3306 from outside the VPC. Set to your laptop's public IP/32 for `bash deploy/scripts/deploy.sh` to be able to create schemas."
  type        = string
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t4g.micro"
}

variable "db_allocated_storage" {
  description = "RDS allocated storage in GB"
  type        = number
  default     = 20
}

variable "db_master_username" {
  description = "RDS master username (Terraform-managed)"
  type        = string
  default     = "admin"
}

variable "db_max_open_conns" {
  description = "Per-service MySQL driver max open connections (sql.DB.SetMaxOpenConns). Total across N service tasks must stay under RDS max_connections — db.t4g.micro defaults to ~60."
  type        = number
  default     = 25
}

variable "db_max_idle_conns" {
  description = "Per-service MySQL driver max idle connections (sql.DB.SetMaxIdleConns)."
  type        = number
  default     = 5
}

variable "db_conn_max_lifetime_min" {
  description = "Per-service MySQL driver connection max lifetime in minutes (sql.DB.SetConnMaxLifetime)."
  type        = number
  default     = 5
}

variable "mq_instance_type" {
  description = "AWS MQ broker instance type"
  type        = string
  default     = "mq.m7g.medium"
}

variable "mq_engine_version" {
  description = "AWS MQ RabbitMQ engine version"
  type        = string
  default     = "3.13"
}

variable "mq_username" {
  description = "Application username for RabbitMQ"
  type        = string
  default     = "appuser"
}

variable "fargate_cpu" {
  description = "Fargate task CPU units (256 = 0.25 vCPU)"
  type        = number
  default     = 256
}

variable "fargate_memory" {
  description = "Fargate task memory in MB"
  type        = number
  default     = 512
}

variable "image_tag" {
  description = "Container image tag pushed by build-and-push.sh"
  type        = string
  default     = "latest"
}

variable "log_retention_days" {
  description = "CloudWatch log group retention in days"
  type        = number
  default     = 7
}
