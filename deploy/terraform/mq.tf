resource "aws_mq_broker" "rabbitmq" {
  broker_name        = "${local.name_prefix}-rabbitmq"
  engine_type        = "RabbitMQ"
  engine_version     = var.mq_engine_version
  host_instance_type = var.mq_instance_type
  deployment_mode    = "SINGLE_INSTANCE"

  publicly_accessible        = false
  auto_minor_version_upgrade = true
  apply_immediately          = true

  # SINGLE_INSTANCE requires exactly one subnet.
  subnet_ids      = [aws_subnet.private[0].id]
  security_groups = [aws_security_group.mq.id]

  user {
    username = var.mq_username
    password = random_password.mq.result
  }

  logs {
    general = true
  }

  tags = { Name = "${local.name_prefix}-rabbitmq" }
}
