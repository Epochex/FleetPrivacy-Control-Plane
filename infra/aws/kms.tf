data "aws_caller_identity" "current" {}

resource "aws_kms_key" "data" {
  description             = "FleetPrivacy database, queue, secret and artifact encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "data" {
  name          = "alias/${var.name}-data"
  target_key_id = aws_kms_key.data.key_id
}
