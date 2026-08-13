# FleetPrivacy AWS Terraform

This stack creates the private production data plane described in
[`docs/aws-production.md`](../../docs/aws-production.md): three-AZ networking,
EKS API and workers, RDS PostgreSQL Multi-AZ, ElastiCache Redis shared connector
admission, SQS with a dead-letter queue, KMS-encrypted S3 artifacts, Secrets
Manager injection through Pod Identity, and CloudWatch alarms.

```bash
cp backend.hcl.example backend.hcl
cp terraform.tfvars.example terraform.tfvars
terraform init -backend-config=backend.hcl
terraform fmt -check -recursive
terraform validate
terraform plan
```

Set `container_image` to an immutable digest and `regional_source_base_url` to
the private HTTPS endpoint serving the five connected-device domains. Run Terraform from a network that
can reach the private EKS endpoint. The Helm provider invokes `aws eks get-token`,
so AWS CLI credentials must belong to the infrastructure deployment role.
