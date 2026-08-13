# AWS 生产部署方案

FleetPrivacy 的生产链路围绕一条可恢复的数据状态机展开：API 将隐私请求、五路数据源任务、Outbox 事件和首条审计记录写入同一 PostgreSQL 事务；Outbox relay 把已提交任务投递到 SQS；worker 依据消息中的 `task_id` 原子领取数据库任务，执行成功后确认消息；Access 制品写入启用 SSE-KMS 的 S3，并把对象 URI 与 SHA-256 摘要写回请求记录。数据库保存业务状态，SQS保存待唤醒消息，S3保存交付制品，三个状态对象各有单一职责。

## 生产需求与技术闭环

| 生产需求 | 输入 | 持久化状态与机制 | 输出 | 故障处理 |
| --- | --- | --- | --- | --- |
| 多租户隐私请求受理 | 租户、请求类型、身份标识、幂等键 | RDS PostgreSQL Multi-AZ 保存请求、任务、Outbox 和审计链；唯一约束收敛客户端并发重试 | 稳定的 request ID 与五路任务 | API 在提交后超时时，调用方使用相同幂等键重试并取得原 request ID |
| 异步任务分发 | 已提交且未发布的 Outbox 行 | relay 使用 `FOR UPDATE SKIP LOCKED` 并行扫描；SQS 标准队列长轮询；可见性超时为 300 秒 | worker 获得带 `task_id` 的事件 | relay 在发送后、标记前退出会形成重复消息；数据库任务终态和租约领取吸收重复投递 |
| 长任务执行 | SQS 消息与数据库任务 | worker 写入 owner、lease、attempt；每 60 秒延长消息可见性；数据库租约为 240 秒 | 数据源回执与请求终态 | worker 失联后，SQS 重新投递消息；数据库租约到期后任务可被其他 worker 领取；连续五次失败进入 DLQ |
| 多 Pod 自适应限流 | 数据源调用的成功、429、超时和服务端错误 | ElastiCache Redis 以 `tenant + source` 保存 AIMD 并发窗口和 active-holder sorted set，Lua 原子更新窗口与准入 lease | worker 取得区域调用许可和下一窗口 | Redis Multi-AZ 自动故障转移；准入 lease 到期后自动释放失联 Worker 占用 |
| Access 制品交付 | 五路成功回执 | S3 对象键为 `artifacts/{tenant_id}/{request_id}.json`；对象使用 KMS 数据密钥加密并记录内容摘要；bucket versioning 保存覆盖历史 | 五分钟有效的预签名下载地址 | 上传失败时保留 SQS 消息；重复消息检测缺失 URI 后覆盖相同对象键并完成聚合；CloudWatch 通过队列积压识别持续依赖故障 |
| Delete 完成验证 | 租户、身份摘要、数据源范围 | PostgreSQL 在租户条件内软删除并反向查询活动记录；审计链记录序号、前序哈希和载荷哈希 | 每个数据源的删除数量和剩余数量 | 反向查询发现剩余记录时任务失败并进入重试链，DLQ 保存超过阈值的消息供修复后重放 |
| 审计与保存期 | 请求状态变化、任务回执、制品 | PostgreSQL advisory transaction lock 串行更新租户链头；S3 versioning 与生命周期联合管理制品 | 可重算的租户审计链和到期删除记录 | 链重算失败触发业务告警；S3 在 30 天后删除当前制品，7 天后清理非当前版本 |

这套闭环处理了标准队列的至少一次投递语义。SQS 消息负责唤醒，数据库任务行决定执行资格。消息顺序变化、重复投递和 worker 重启都落到同一条原子领取语句，业务副作用由任务终态约束。

## 服务责任矩阵

| AWS 服务 | 输入 | 持有状态 | 输出 | 异常后的恢复点 |
| --- | --- | --- | --- | --- |
| EKS | 发布镜像、Pod 配置、请求流量、SQS 消息 | Deployment 副本、Pod 调度与健康状态 | 可用 API 实例和 worker 实例 | readiness 摘除故障 API；Deployment 重建 worker；PDB 保证节点维护期间保留服务副本 |
| RDS PostgreSQL | 请求命令、任务领取、执行回执、Outbox 与审计事件 | 请求状态机、任务租约、幂等键、Outbox、数据记录、租户审计链 | 原子提交结果和可恢复任务真相源 | Multi-AZ 切换连接端点；连接池重新建连；未提交事务回滚后由相同幂等键或任务租约继续 |
| SQS | Outbox relay 发布的 task ID 事件 | 消息可见性、接收次数、主队列与 DLQ | worker 唤醒信号 | 可见性到期重新投递；第五次失败转入 DLQ；修复依赖后按 event ID redrive |
| ElastiCache Redis | 各连接器的成功、429、超时、服务端错误和延迟样本 | 租户与数据源粒度的 AIMD 窗口和限时 active-holder 集合 | 区域调用许可与下一批共享并发窗口 | 副本自动接管；准入 lease 和键 TTL 清理失联任务与停用数据源 |
| S3 | Access JSON 字节、租户 ID、request ID、内容摘要 | KMS 加密的版本化对象和对象 metadata | 预签名下载对象 | 相同对象键重试上传；生命周期清理过期版本；摘要校验发现传输或读取差异 |
| KMS | RDS、Redis、SQS、S3、Secrets Manager 的加解密请求 | 轮换密钥版本和授权策略 | 数据密钥或解密结果 | CloudTrail 定位拒绝主体；恢复 IAM/KMS policy 后原请求可重试 |
| Secrets Manager | Terraform 生成的数据库 URL、Redis URL、API key、webhook secret 和区域服务令牌 | KMS 加密的 secret 版本 | CSI Driver 挂载并同步的 Kubernetes Secret | Pod 保留已挂载值完成在途请求；恢复读取权限后滚动 Pod 获取新版本 |
| CloudWatch | RDS、Redis、SQS 指标和 EKS control-plane 日志 | 告警窗口、日志事件和处置入口 | SNS 值班通知与故障时间线 | 缺失指标按资源类型设为 breaching 或 notBreaching；告警恢复后保留状态变化记录 |

## AWS 资源布局

Terraform 位于 `infra/aws`，默认创建三可用区生产拓扑：

- VPC 将负载均衡、EKS 节点和 RDS 分别放入 public、private、database subnet；每个可用区配置独立 NAT gateway，单区网络故障不会切断其余 worker 的依赖访问。
- EKS 1.36 使用三个按需 ARM 节点承载 API 与 worker。API 三副本跨可用区分布，滚动发布保持零不可用副本，HPA 在平均 CPU 达到 65% 后扩展至 12 副本。
- RDS PostgreSQL 16.14 使用 Multi-AZ、gp3 100 GiB、500 GiB 自动扩容上限、35 天备份、删除保护、Performance Insights 和强制 TLS。安全组只接受 EKS 节点到 5432 端口的连接。
- ElastiCache Redis 7.1 使用一主两副本、跨可用区自动故障转移、TLS、AUTH、KMS 静态加密和 7 天快照。它接收各 worker 的数据源调用反馈，维护跨 Pod 共享的 AIMD 窗口和 active-holder 准入 lease，输出区域调用许可；任务状态与审计记录落在 PostgreSQL。
- SQS 主队列使用 20 秒长轮询、300 秒可见性窗口和五次接收阈值；DLQ 保存失败消息 14 天，支持修复依赖后按原事件重放。
- S3 拒绝公共访问和明文传输，使用 KMS bucket key 降低数据密钥调用量；版本控制保存覆盖记录，生命周期执行制品保存期。
- Secrets Manager 保存数据库 URL、Redis URL、API key、webhook secret 和区域服务令牌。Secrets Store CSI Driver 把指定 JSON 字段同步为 Pod 环境变量，密钥不进入镜像和 Helm values。
- KMS 每年自动轮换数据密钥，统一加密 RDS、S3、SQS 和 Secrets Manager。资源策略和 IAM policy 只允许当前工作负载访问指定 ARN。

AWS 当前将 EKS 1.36 列为标准支持版本；RDS PostgreSQL 16.14 处于支持列表。上线前仍通过 AWS CLI 查询目标 Region 的可用版本，随后把选定版本固化到变更单。版本依据见 [EKS Kubernetes 生命周期](https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html) 与 [RDS PostgreSQL 版本](https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-versions.html)。

## 身份与最小权限

EKS Pod Identity 把 IAM role 绑定到 namespace 与 service account。AWS SDK 从 Pod Identity Agent 获取短期凭据，容器内没有长期 access key。官方机制说明见 [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html)。

两个工作负载使用独立权限集：

| Service account | 允许操作 | 资源范围 |
| --- | --- | --- |
| `fleetprivacy-api` | 读取应用 secret、读取 Access 制品、KMS 解密 | 单个 Secrets Manager secret、`artifacts/*` 对象、单个 KMS key |
| `fleetprivacy-worker` | 读取应用 secret、收发与确认主队列消息、写入 Access 制品、使用数据密钥 | 单个主队列、`artifacts/*` 对象、单个 KMS key |

DLQ 没有授予应用读取权限。值班工程师通过独立的故障恢复角色检查和重放 DLQ，生产 worker 无法绕过重放审批。Secrets Store CSI 与 Pod Identity 的组合方式遵循 [AWS ASCP 集成说明](https://docs.aws.amazon.com/secretsmanager/latest/userguide/ascp-pod-identity-integration.html)。

## 监控与告警动作

CloudWatch 告警直接对应需要处置的状态：

| 信号 | 阈值 | 表达的状态 | 值班动作 |
| --- | --- | --- | --- |
| RDS CPU | 15 分钟平均值高于 80% | 查询、连接或任务写入压力持续增长 | 检查 Performance Insights 的等待事件和慢 SQL；确认索引命中后再调整实例容量 |
| RDS free storage | 连续 10 分钟低于 20 GiB | WAL、表膨胀或任务增长逼近写入安全水位 | 检查增长表与 autovacuum；校准自动扩容上限 |
| Redis engine CPU | 15 分钟平均值高于 75% | AIMD 窗口和退避键更新开始排队 | 检查 hot key、连接数与 Lua 脚本耗时；按节点 CPU 和内存曲线扩容 |
| Redis evictions | 5 分钟内大于 0 | 控制状态被内存策略提前淘汰 | 检查键 TTL 和租户数据源基数；扩容并确认淘汰归零 |
| SQS oldest message | 连续 5 分钟超过 300 秒 | worker 吞吐低于任务到达速率，或下游依赖持续超时 | 比较任务数据源耗时和 worker 副本数；扩容后确认队列年龄下降 |
| DLQ visible messages | 大于 0 | 消息已连续失败五次 | 按 event ID 定位数据库任务和错误回执；修复依赖后执行受控 redrive |

API `/metrics` 继续输出请求完成计数和任务耗时直方图。平台采集器按 `tenant_id` 以外的稳定标签聚合，避免把高基数租户标识写入指标维度。EKS control-plane 的 API、audit、authenticator、controller manager 和 scheduler 日志全部启用。

## 部署

先准备启用 versioning、SSE-KMS 和锁表的 Terraform state bucket。生产 state 保存随机生成的数据库凭据，因此 state bucket 访问权限定在平台发布角色。

```bash
cd infra/aws
cp backend.hcl.example backend.hcl
cp terraform.tfvars.example terraform.tfvars
terraform init -backend-config=backend.hcl
terraform fmt -check -recursive
terraform validate
terraform plan -out=production.tfplan
terraform apply production.tfplan
```

EKS API endpoint 默认只开放私网。执行 Helm 资源的 Terraform runner 需要位于 VPC、对等网络或企业 VPN 内，并安装 AWS CLI。`container_image` 使用发布流水线生成的 digest，发布审批记录 digest、数据库迁移版本和回滚版本。

部署后检查状态：

```bash
aws eks update-kubeconfig --name fleetprivacy-prod --region eu-west-1
kubectl -n fleetprivacy rollout status deployment/fleetprivacy-api
kubectl -n fleetprivacy rollout status deployment/fleetprivacy-worker
kubectl -n fleetprivacy get pods,service,hpa,pdb
kubectl -n fleetprivacy exec deploy/fleetprivacy-api -- \
  python -c 'import boto3; print(boto3.client("sts").get_caller_identity()["Arn"])'
```

## LocalStack 集成验证

`compose.aws-test.yml` 启动 PostgreSQL 16、Redis 7、LocalStack S3/SQS/KMS、API 和 SQS worker。初始化脚本创建加密 bucket、主队列、DLQ 和 redrive policy，Redis 容器验证跨进程 AIMD 窗口与退避状态，测试环境复用与生产一致的 adapter。

```bash
docker compose -f compose.aws-test.yml up --build --abort-on-container-exit
```

集成测试至少覆盖四条链路：创建请求后 Outbox 事件进入主队列；worker 执行并确认消息；Access 制品带 `aws:kms` 加密属性和 SHA-256 metadata；让处理函数连续失败五次后消息进入 DLQ。验证结束后检查主队列深度归零、请求进入终态、数据库 artifact URI 指向对应 S3 object key。

## 故障演练

每个发布周期执行以下演练并保存时间线：

1. 在任务执行中终止 worker Pod，验证消息可见性恢复、数据库租约到期和另一 worker 完成任务。
2. 暂停 Outbox relay 的 SQS 权限，验证 Outbox 行保持未发布；恢复权限后事件全部补发。
3. 临时拒绝 S3 PutObject，验证 SQS 消息保持未确认且请求缺少制品 URI；恢复后重复消息生成制品且任务 attempt 不增加。
4. 触发 RDS Multi-AZ failover，记录连接池恢复时间、API 错误率和队列最大年龄。
5. 投递固定失败事件进入 DLQ，修复依赖后执行 redrive，并核对 task attempt、终态和审计链。

故障演练的完成条件是数据库请求终态、SQS 消息状态、S3 制品摘要和审计链四处数据能够按 request ID 相互核对。
