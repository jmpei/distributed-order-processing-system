resource "random_password" "db" {
  length  = 24
  special = false
}

resource "random_password" "mq" {
  length  = 24
  special = false
}

# ── Database credentials ───────────────────────────────────────

resource "aws_secretsmanager_secret" "db" {
  name                    = "${local.name_prefix}-db-credentials"
  description             = "RDS MySQL master credentials"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = var.db_master_username
    password = random_password.db.result
    host     = aws_db_instance.this.address
    port     = aws_db_instance.this.port
  })
}

# ── RabbitMQ credentials + full amqps URL ──────────────────────

locals {
  # aws_mq_broker.this.instances[0].endpoints[0] is e.g.
  #   "amqps://b-abcd.mq.us-west-2.amazonaws.com:5671"
  # Strip the scheme so we can inject user:pass.
  mq_host_port = replace(aws_mq_broker.rabbitmq.instances[0].endpoints[0], "amqps://", "")
  mq_url       = "amqps://${var.mq_username}:${random_password.mq.result}@${local.mq_host_port}/"
}

resource "aws_secretsmanager_secret" "mq" {
  name                    = "${local.name_prefix}-mq-credentials"
  description             = "AWS MQ RabbitMQ application credentials"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "mq" {
  secret_id = aws_secretsmanager_secret.mq.id
  secret_string = jsonencode({
    username = var.mq_username
    password = random_password.mq.result
    url      = local.mq_url
  })
}
