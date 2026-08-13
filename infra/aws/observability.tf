resource "aws_cloudwatch_metric_alarm" "database_cpu" {
  alarm_name          = "${var.name}-database-cpu"
  alarm_description   = "Sustained database CPU indicates query or worker pressure before request latency grows."
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = 80
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = var.alarm_topic_arns
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.postgres.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "database_free_storage" {
  alarm_name          = "${var.name}-database-free-storage"
  alarm_description   = "Less than 20 GiB free triggers capacity investigation before PostgreSQL stops accepting writes."
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 21474836480
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = var.alarm_topic_arns
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.postgres.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "queue_age" {
  alarm_name          = "${var.name}-oldest-request"
  alarm_description   = "The oldest wake-up exceeded five minutes; scale workers or inspect a poisoned dependency."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_topic_arns
  dimensions = {
    QueueName = aws_sqs_queue.requests.name
  }
}

resource "aws_cloudwatch_metric_alarm" "dead_letter_depth" {
  alarm_name          = "${var.name}-dead-letter-depth"
  alarm_description   = "At least one request exhausted five deliveries and needs replay after root-cause repair."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_topic_arns
  dimensions = {
    QueueName = aws_sqs_queue.dead_letter.name
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_cpu" {
  alarm_name          = "${var.name}-redis-engine-cpu"
  alarm_description   = "Sustained Redis command pressure can delay concurrency-window and backoff updates."
  namespace           = "AWS/ElastiCache"
  metric_name         = "EngineCPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = 75
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = var.alarm_topic_arns
  dimensions = {
    ReplicationGroupId = aws_elasticache_replication_group.redis.id
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_evictions" {
  alarm_name          = "${var.name}-redis-evictions"
  alarm_description   = "Evicted connector control keys indicate insufficient cache memory or invalid TTL policy."
  namespace           = "AWS/ElastiCache"
  metric_name         = "Evictions"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_topic_arns
  dimensions = {
    ReplicationGroupId = aws_elasticache_replication_group.redis.id
  }
}
