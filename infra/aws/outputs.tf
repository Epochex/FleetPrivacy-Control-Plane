output "cluster_name" {
  value = module.eks.cluster_name
}

output "database_endpoint" {
  value = aws_db_instance.postgres.endpoint
}

output "request_queue_url" {
  value = aws_sqs_queue.requests.url
}

output "dead_letter_queue_url" {
  value = aws_sqs_queue.dead_letter.url
}

output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.id
}

output "application_secret_arn" {
  value = aws_secretsmanager_secret.application.arn
}

output "redis_primary_endpoint" {
  value = aws_elasticache_replication_group.redis.primary_endpoint_address
}
