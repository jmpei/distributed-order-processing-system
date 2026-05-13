#!/usr/bin/env bash
# Terraform apply in two stages so per-service DB schemas can be created
# between bringing up RDS and starting ECS services.
#
# Stage 1: bring up VPC + RDS + Secrets + ECR (no ECS yet)
# Stage 2: create inventory_db / payments_db schemas via mysql CLI
# Stage 3: bring up MQ + ALB + ECS (full apply)
#
# Total wall time: ~10-15 min (MQ broker creation dominates).
#
# Prereqs:
#   - aws CLI + terraform + mysql client installed
#   - deploy/terraform/terraform.tfvars exists (copy from .example)
#   - bash deploy/scripts/build-and-push.sh has run OR will run before
#     ECS tasks need to start (ECS will retry pulling until image exists)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="$REPO_ROOT/deploy/terraform"

if [[ ! -f "$TF_DIR/terraform.tfvars" ]]; then
  echo "✗ $TF_DIR/terraform.tfvars not found."
  echo "  Run: cp $TF_DIR/terraform.tfvars.example $TF_DIR/terraform.tfvars"
  echo "  Then edit it to set admin_cidr to your public IP."
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "✗ docker not found. Stage 2 runs mysql:8 in a container to talk to RDS."
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "✗ Docker daemon not running. Start Docker Desktop and retry."
  exit 1
fi

cd "$TF_DIR"

echo "▶ terraform init"
terraform init -input=false

echo
echo "▶ Stage 1/3: apply VPC + RDS + Secrets + ECR (~5 min for RDS)"
terraform apply -input=false -auto-approve \
  -target=aws_db_instance.this \
  -target=aws_secretsmanager_secret_version.db \
  -target=aws_ecr_repository.this

DB_HOST=$(terraform output -raw rds_endpoint)
DB_PORT=$(terraform output -raw rds_port)
# db_master_username output only references var.db_master_username; a partial
# apply with -target won't materialise it in state. Evaluate the var directly.
DB_USER=$(echo 'var.db_master_username' | terraform console | tr -d '"')
DB_PASS=$(terraform output -raw db_master_password)

echo
echo "▶ Stage 2/3: creating inventory_db and payments_db schemas"
# orders_db is created by RDS as the initial DB. The other two we add now.
# Use mysql:8 in a container — Homebrew's mysql 9.x dropped the
# mysql_native_password plugin that RDS 8.0 still uses for the master user.
docker run --rm \
  -e MYSQL_PWD="$DB_PASS" \
  mysql:8 \
  mysql \
    --host="$DB_HOST" \
    --port="$DB_PORT" \
    --user="$DB_USER" \
    --connect-timeout=10 \
    -e "CREATE DATABASE IF NOT EXISTS inventory_db CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
        CREATE DATABASE IF NOT EXISTS payments_db CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
        SHOW DATABASES;"

echo
echo "▶ Stage 3/3: apply remaining resources (MQ + ALB + ECS — ~10 min for MQ broker)"
terraform apply -input=false -auto-approve

echo
echo "=================================================================="
echo "✓ Deploy complete."
echo
echo "ALB URL:        $(terraform output -raw alb_url)"
echo "RDS endpoint:   $(terraform output -raw rds_endpoint)"
echo "MQ console:     $(terraform output -raw mq_console_url)"
echo "ECS cluster:    $(terraform output -raw ecs_cluster_name)"
echo
echo "Note: ECS tasks need a few minutes to register healthy with the ALB."
echo "Watch task health:"
echo "  aws ecs describe-services \\"
echo "    --cluster  $(terraform output -raw ecs_cluster_name) \\"
echo "    --services dop-dev-order dop-dev-inventory dop-dev-payment \\"
echo "    --query 'services[].{name:serviceName,running:runningCount,desired:desiredCount}'"
echo
echo "Sanity-check the API once tasks are healthy (returns JSON list):"
echo "  curl -s $(terraform output -raw alb_url)/orders"
echo "  ($(terraform output -raw alb_url)/health hits the ALB default 404;"
echo "   /health is only served by each task internally.)"
echo
echo "⚠ This deployment costs ~\$2.85/day. Run destroy.sh when done."
echo "=================================================================="
