variable "aws_region" {
  description = "AWS region containing the private data plane."
  type        = string
  default     = "eu-west-1"
}

variable "name" {
  description = "Prefix applied to infrastructure names."
  type        = string
  default     = "fleetprivacy-prod"
}

variable "vpc_cidr" {
  description = "Address space split across three availability zones."
  type        = string
  default     = "10.42.0.0/16"
}

variable "kubernetes_version" {
  description = "EKS control-plane version."
  type        = string
  default     = "1.36"
}

variable "container_image" {
  description = "Immutable FleetPrivacy image reference, preferably pinned by digest."
  type        = string
}

variable "regional_source_base_url" {
  description = "Private HTTPS base URL for regional connected-device source APIs."
  type        = string
}

variable "api_replicas" {
  description = "Steady-state API replica count before horizontal autoscaling."
  type        = number
  default     = 3
}

variable "database_instance_class" {
  description = "RDS instance class for the Multi-AZ primary and standby."
  type        = string
  default     = "db.r6g.large"
}

variable "database_name" {
  description = "PostgreSQL database used by the workflow state machine."
  type        = string
  default     = "fleetprivacy"
}

variable "database_username" {
  description = "Application database owner stored in Secrets Manager."
  type        = string
  default     = "fleetprivacy"
}

variable "alarm_topic_arns" {
  description = "SNS topics receiving RDS, queue and dead-letter alarms."
  type        = list(string)
  default     = []
}

variable "artifact_retention_days" {
  description = "Days before completed access packages expire from S3."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Additional ownership and cost-allocation tags."
  type        = map(string)
  default     = {}
}
