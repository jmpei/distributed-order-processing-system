output "alb_dns_name" {
  description = "Public DNS name of the ALB — use this as the base URL for curl / Locust"
  value       = aws_lb.this.dns_name
}

output "alb_url" {
  description = "Convenience: http:// + ALB DNS"
  value       = "http://${aws_lb.this.dns_name}"
}

output "rds_endpoint" {
  description = "RDS MySQL hostname (no port)"
  value       = aws_db_instance.this.address
}

output "rds_port" {
  description = "RDS MySQL port"
  value       = aws_db_instance.this.port
}

output "mq_endpoint" {
  description = "AMQPS endpoint URL (host:port included)"
  value       = aws_mq_broker.rabbitmq.instances[0].endpoints[0]
}

output "mq_console_url" {
  description = "RabbitMQ web console URL (login with mq creds)"
  value       = aws_mq_broker.rabbitmq.instances[0].console_url
}

output "ecr_repository_urls" {
  description = "ECR repo URLs keyed by service — used by build-and-push.sh"
  value       = { for k, v in aws_ecr_repository.this : k => v.repository_url }
}

output "ecs_cluster_name" {
  description = "ECS cluster name — used by deploy.sh for force-new-deployment"
  value       = aws_ecs_cluster.this.name
}

output "ecs_service_names" {
  description = "ECS service names keyed by service"
  value       = { for k, v in aws_ecs_service.service : k => v.name }
}

output "db_master_username" {
  description = "RDS master username"
  value       = var.db_master_username
}

output "db_master_password" {
  description = "RDS master password (sensitive — use `terraform output -raw db_master_password`)"
  value       = random_password.db.result
  sensitive   = true
}

output "mq_username" {
  description = "RabbitMQ application username"
  value       = var.mq_username
}

output "mq_password" {
  description = "RabbitMQ application password (sensitive)"
  value       = random_password.mq.result
  sensitive   = true
}
