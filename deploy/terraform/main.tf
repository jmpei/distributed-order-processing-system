terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Local backend by design — keeps Phase 6 self-contained, no bootstrap S3
  # bucket / DynamoDB lock table required. terraform.tfstate is gitignored.
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)

  name_prefix = "${var.project}-${var.environment}"

  services = {
    order = {
      port          = 8081
      db_name       = "orders_db"
      port_env_name = "ORDER_PORT"
      extra_env     = {}
      path_patterns = ["/orders", "/orders/*", "/admin", "/admin/*"]
      listener_prio = 10
      ecr_repo_name = "order-service"
    }
    inventory = {
      port          = 8082
      db_name       = "inventory_db"
      port_env_name = "INVENTORY_PORT"
      extra_env = {
        INVENTORY_RESERVE_MAX_RETRIES = "50"
      }
      path_patterns = ["/inventory", "/inventory/*"]
      listener_prio = 20
      ecr_repo_name = "inventory-service"
    }
    payment = {
      port          = 8083
      db_name       = "payments_db"
      port_env_name = "PAYMENT_PORT"
      extra_env = {
        PAYMENT_FAILURE_RATE = "0.0"
      }
      path_patterns = ["/payments", "/payments/*"]
      listener_prio = 30
      ecr_repo_name = "payment-service"
    }
  }
}
