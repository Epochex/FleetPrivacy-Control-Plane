module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.21.0"

  name = var.name
  cidr = var.vpc_cidr
  azs  = local.azs

  public_subnets   = [for index, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, index)]
  private_subnets  = [for index, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, index + 3)]
  database_subnets = [for index, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, index + 6)]

  enable_nat_gateway     = true
  one_nat_gateway_per_az = true
  single_nat_gateway     = false
  enable_dns_hostnames   = true
  enable_dns_support     = true

  create_database_subnet_group = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

resource "aws_security_group" "database" {
  name_prefix = "${var.name}-postgres-"
  description = "PostgreSQL accepts sessions only from FleetPrivacy EKS nodes"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "asyncpg sessions from application pods"
    from_port       = 5432
    to_port         = 5432
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
