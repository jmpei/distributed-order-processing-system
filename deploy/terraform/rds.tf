# RDS lives in public subnets because publicly_accessible=true requires
# IGW routing on the subnet group. SG locks ingress to admin_cidr (laptop)
# and the ECS SG — see vpc.tf. For prod, flip to private subnets + remove
# publicly_accessible, and reach the DB via SSM Session Manager / bastion.
resource "aws_db_subnet_group" "this" {
  name       = "${local.name_prefix}-db-subnets"
  subnet_ids = aws_subnet.public[*].id

  tags = { Name = "${local.name_prefix}-db-subnets" }
}

resource "aws_db_instance" "this" {
  identifier     = "${local.name_prefix}-mysql"
  engine         = "mysql"
  engine_version = "8.0"
  instance_class = var.db_instance_class

  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = var.db_allocated_storage * 2
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "orders_db" # initial schema; inventory_db/payments_db created by deploy.sh
  username = var.db_master_username
  password = random_password.db.result
  port     = 3306

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true

  multi_az                = false
  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true

  performance_insights_enabled = false
  monitoring_interval          = 0

  tags = { Name = "${local.name_prefix}-mysql" }
}
