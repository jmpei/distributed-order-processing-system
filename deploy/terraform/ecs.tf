resource "aws_ecs_cluster" "this" {
  name = "${local.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "disabled" # extra cost; off for dev
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }
}

resource "aws_cloudwatch_log_group" "service" {
  for_each = local.services

  name              = "/ecs/${local.name_prefix}/${each.key}"
  retention_in_days = var.log_retention_days
}

locals {
  # Common (non-secret) env vars every service gets.
  common_env = [
    { name = "DB_HOST", value = aws_db_instance.this.address },
    { name = "DB_PORT", value = tostring(aws_db_instance.this.port) },
    { name = "DB_MAX_OPEN_CONNS", value = tostring(var.db_max_open_conns) },
    { name = "DB_MAX_IDLE_CONNS", value = tostring(var.db_max_idle_conns) },
    { name = "DB_CONN_MAX_LIFETIME_MIN", value = tostring(var.db_conn_max_lifetime_min) },
  ]

  # Common secrets: DB creds (extracted via JSON key) + full amqps URL.
  common_secrets = [
    { name = "DB_USER", valueFrom = "${aws_secretsmanager_secret.db.arn}:username::" },
    { name = "DB_PASS", valueFrom = "${aws_secretsmanager_secret.db.arn}:password::" },
    { name = "RABBITMQ_URL", valueFrom = "${aws_secretsmanager_secret.mq.arn}:url::" },
  ]
}

resource "aws_ecs_task_definition" "service" {
  for_each = local.services

  family                   = "${local.name_prefix}-${each.key}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.fargate_cpu
  memory                   = var.fargate_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = each.key
    image     = "${aws_ecr_repository.this[each.key].repository_url}:${var.image_tag}"
    essential = true

    portMappings = [{
      containerPort = each.value.port
      protocol      = "tcp"
    }]

    environment = concat(
      local.common_env,
      [
        { name = each.value.port_env_name, value = tostring(each.value.port) },
        { name = "DB_NAME", value = each.value.db_name },
      ],
      [for k, v in each.value.extra_env : { name = k, value = v }],
    )

    secrets = local.common_secrets

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.service[each.key].name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}

resource "aws_ecs_service" "service" {
  for_each = local.services

  name            = "${local.name_prefix}-${each.key}"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.service[each.key].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.service[each.key].arn
    container_name   = each.key
    container_port   = each.value.port
  }

  # Avoid Terraform fighting CodeDeploy / re-deploys; image pushes are
  # promoted via `aws ecs update-service --force-new-deployment`.
  lifecycle {
    ignore_changes = [desired_count]
  }

  depends_on = [aws_lb_listener.http]
}
