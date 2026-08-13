resource "aws_elasticache_subnet_group" "redis" {
  name       = var.name
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "redis" {
  name_prefix = "${var.name}-redis-"
  description = "Redis TLS sessions from FleetPrivacy EKS nodes"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "TLS Redis sessions from worker pods"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = var.name
  description          = "FleetPrivacy shared connector control state"

  engine               = "redis"
  engine_version       = "7.1"
  node_type            = "cache.r7g.large"
  port                 = 6379
  parameter_group_name = "default.redis7"

  num_cache_clusters         = 3
  multi_az_enabled           = true
  automatic_failover_enabled = true

  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  security_group_ids = [aws_security_group.redis.id]

  transit_encryption_enabled = true
  auth_token                 = random_password.redis_auth.result
  at_rest_encryption_enabled = true
  kms_key_id                 = aws_kms_key.data.arn

  snapshot_retention_limit   = 7
  snapshot_window            = "01:00-02:00"
  maintenance_window         = "sun:04:30-sun:05:30"
  auto_minor_version_upgrade = true
  apply_immediately          = false
}
