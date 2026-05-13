resource "aws_lb" "this" {
  name               = "${local.name_prefix}-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.alb.id]

  idle_timeout = 60

  tags = { Name = "${local.name_prefix}-alb" }
}

resource "aws_lb_target_group" "service" {
  for_each = local.services

  name        = "${local.name_prefix}-${each.key}-tg"
  port        = each.value.port
  protocol    = "HTTP"
  target_type = "ip" # Fargate awsvpc tasks register by ENI IP
  vpc_id      = aws_vpc.this.id

  health_check {
    path                = "/health"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
    matcher             = "200"
  }

  deregistration_delay = 30
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "fixed-response"
    fixed_response {
      content_type = "text/plain"
      message_body = "Not Found"
      status_code  = "404"
    }
  }

  # Production-ready HTTPS hook: add an additional aws_lb_listener on 443
  # with `protocol = "HTTPS"` + `certificate_arn = aws_acm_certificate...`,
  # and redirect this listener to it. Intentionally omitted in dev to skip
  # ACM cert provisioning + Route53 dependency.
}

resource "aws_lb_listener_rule" "service" {
  for_each = local.services

  listener_arn = aws_lb_listener.http.arn
  priority     = each.value.listener_prio

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.service[each.key].arn
  }

  condition {
    path_pattern {
      values = each.value.path_patterns
    }
  }
}
