module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "20.37.1"

  cluster_name    = var.name
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access           = false
  cluster_endpoint_private_access          = true
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_enabled_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  cluster_addons = {
    coredns                = { most_recent = true }
    kube-proxy             = { most_recent = true }
    vpc-cni                = { most_recent = true, before_compute = true }
    eks-pod-identity-agent = { most_recent = true, before_compute = true }
  }

  eks_managed_node_groups = {
    application = {
      instance_types = ["m7g.large"]
      ami_type       = "AL2023_ARM_64_STANDARD"
      min_size       = 3
      desired_size   = 3
      max_size       = 9
      capacity_type  = "ON_DEMAND"
      subnet_ids     = module.vpc.private_subnets

      labels = {
        workload = "fleetprivacy"
      }
    }
  }
}

data "aws_iam_policy_document" "pod_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "worker" {
  name               = "${var.name}-worker"
  assume_role_policy = data.aws_iam_policy_document.pod_assume.json
}

data "aws_iam_policy_document" "worker" {
  statement {
    sid = "ConsumeRequestWakeups"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:ReceiveMessage",
      "sqs:SendMessage"
    ]
    resources = [aws_sqs_queue.requests.arn]
  }

  statement {
    sid       = "ListArtifactPrefix"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["artifacts/*"]
    }
  }

  statement {
    sid = "ManageArtifacts"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = ["${aws_s3_bucket.artifacts.arn}/artifacts/*"]
  }

  statement {
    sid       = "ReadApplicationSecret"
    actions   = ["secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.application.arn]
  }

  statement {
    sid = "UseDataKey"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey"
    ]
    resources = [aws_kms_key.data.arn]
  }
}

resource "aws_iam_role_policy" "worker" {
  role   = aws_iam_role.worker.id
  policy = data.aws_iam_policy_document.worker.json
}

resource "aws_eks_pod_identity_association" "worker" {
  cluster_name    = module.eks.cluster_name
  namespace       = "fleetprivacy"
  service_account = "fleetprivacy-worker"
  role_arn        = aws_iam_role.worker.arn
}

resource "aws_iam_role" "api" {
  name               = "${var.name}-api"
  assume_role_policy = data.aws_iam_policy_document.pod_assume.json
}

data "aws_iam_policy_document" "api" {
  statement {
    sid       = "DownloadArtifacts"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/artifacts/*"]
  }

  statement {
    sid       = "ReadApplicationSecret"
    actions   = ["secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.application.arn]
  }

  statement {
    sid       = "DecryptArtifactsAndSecret"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.data.arn]
  }
}

resource "aws_iam_role_policy" "api" {
  role   = aws_iam_role.api.id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_eks_pod_identity_association" "api" {
  cluster_name    = module.eks.cluster_name
  namespace       = "fleetprivacy"
  service_account = "fleetprivacy-api"
  role_arn        = aws_iam_role.api.arn
}

resource "helm_release" "secrets_store_csi" {
  name             = "secrets-store-csi-driver"
  repository       = "https://kubernetes-sigs.github.io/secrets-store-csi-driver/charts"
  chart            = "secrets-store-csi-driver"
  namespace        = "kube-system"
  version          = "1.6.0"
  create_namespace = false

  set {
    name  = "syncSecret.enabled"
    value = "true"
  }

  depends_on = [module.eks]
}

resource "helm_release" "secrets_store_aws" {
  name       = "secrets-store-csi-driver-provider-aws"
  repository = "https://aws.github.io/secrets-store-csi-driver-provider-aws"
  chart      = "secrets-store-csi-driver-provider-aws"
  namespace  = "kube-system"
  version    = "3.1.2"

  set {
    name  = "secrets-store-csi-driver.install"
    value = "false"
  }

  depends_on = [helm_release.secrets_store_csi]
}

resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  namespace  = "kube-system"
  version    = "3.13.1"

  depends_on = [module.eks]
}

resource "helm_release" "application" {
  name             = "fleetprivacy"
  chart            = "${path.module}/../../deploy/aws"
  namespace        = "fleetprivacy"
  create_namespace = true

  values = [yamlencode({
    image    = var.container_image
    replicas = var.api_replicas
    aws = {
      region                = var.aws_region
      queueUrl              = aws_sqs_queue.requests.url
      bucket                = aws_s3_bucket.artifacts.id
      secretArn             = aws_secretsmanager_secret.application.arn
      kmsKeyArn             = aws_kms_key.data.arn
      regionalSourceBaseUrl = var.regional_source_base_url
    }
  })]

  depends_on = [
    aws_eks_pod_identity_association.api,
    aws_eks_pod_identity_association.worker,
    aws_secretsmanager_secret_version.application,
    helm_release.secrets_store_aws,
    helm_release.metrics_server
  ]
}
