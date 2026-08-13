resource "random_password" "database" {
  length           = 32
  special          = true
  override_special = "!#$%&*+-.:=?@_"
}

resource "random_password" "api_key" {
  length  = 48
  special = false
}

resource "random_password" "regional_source_token" {
  length  = 48
  special = false
}

resource "random_password" "redis_auth" {
  length           = 32
  special          = true
  override_special = "!&#$^<>-"
}

resource "aws_db_parameter_group" "postgres" {
  name_prefix = "${var.name}-postgres16-"
  family      = "postgres16"

  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }

  parameter {
    name  = "log_min_duration_statement"
    value = "500"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_instance" "postgres" {
  identifier = var.name

  engine               = "postgres"
  engine_version       = "16.14"
  instance_class       = var.database_instance_class
  db_name              = var.database_name
  username             = var.database_username
  password             = random_password.database.result
  parameter_group_name = aws_db_parameter_group.postgres.name

  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [aws_security_group.database.id]
  publicly_accessible    = false
  multi_az               = true
  port                   = 5432

  storage_type          = "gp3"
  allocated_storage     = 100
  max_allocated_storage = 500
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.data.arn

  backup_retention_period   = 35
  backup_window             = "02:00-03:00"
  maintenance_window        = "sun:03:30-sun:04:30"
  copy_tags_to_snapshot     = true
  deletion_protection       = true
  skip_final_snapshot       = false
  final_snapshot_identifier = "${var.name}-final"

  performance_insights_enabled          = true
  performance_insights_kms_key_id       = aws_kms_key.data.arn
  performance_insights_retention_period = 7
  enabled_cloudwatch_logs_exports       = ["postgresql", "upgrade"]
  auto_minor_version_upgrade            = true
  apply_immediately                     = false
}

resource "aws_secretsmanager_secret" "application" {
  name                    = "${var.name}/application"
  description             = "FleetPrivacy database URL and API authentication material"
  kms_key_id              = aws_kms_key.data.arn
  recovery_window_in_days = 30
}

resource "aws_secretsmanager_secret_version" "application" {
  secret_id = aws_secretsmanager_secret.application.id
  secret_string = jsonencode({
    database_url          = "postgresql+asyncpg://${var.database_username}:${urlencode(random_password.database.result)}@${aws_db_instance.postgres.address}:${aws_db_instance.postgres.port}/${var.database_name}?ssl=require"
    redis_url             = "rediss://:${urlencode(random_password.redis_auth.result)}@${aws_elasticache_replication_group.redis.primary_endpoint_address}:6379/0"
    api_key               = random_password.api_key.result
    webhook_secret        = random_password.api_key.result
    regional_source_token = random_password.regional_source_token.result
  })
}
