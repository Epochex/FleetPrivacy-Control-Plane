resource "aws_sqs_queue" "dead_letter" {
  name                              = "${var.name}-requests-dlq"
  message_retention_seconds         = 1209600
  kms_master_key_id                 = aws_kms_key.data.arn
  kms_data_key_reuse_period_seconds = 300
}

resource "aws_sqs_queue" "requests" {
  name                              = "${var.name}-requests"
  visibility_timeout_seconds        = 300
  message_retention_seconds         = 345600
  receive_wait_time_seconds         = 20
  kms_master_key_id                 = aws_kms_key.data.arn
  kms_data_key_reuse_period_seconds = 300

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dead_letter" {
  queue_url = aws_sqs_queue.dead_letter.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.requests.arn]
  })
}
