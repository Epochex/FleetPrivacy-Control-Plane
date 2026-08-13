locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 3)

  tags = merge({
    Service     = "fleetprivacy"
    Environment = "production"
    ManagedBy   = "terraform"
    DataClass   = "restricted"
  }, var.tags)
}
