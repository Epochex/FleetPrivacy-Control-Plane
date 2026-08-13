# 汉朔联网设备隐私云服务面试深挖

## 60 秒项目介绍

这个项目处理联网设备云中的账号注销及数据主体访问、删除请求。企业客户从合规门户提交一个账号或人员标识，该标识关联的资料分散在账户、设备操作归属、带身份标签的遥测、任务发起历史和支持工单五个区域服务。逐个系统开工单无法统一跟踪完成状态，局部失败后的人工重跑还会重复已经完成的删除。

我用 Python 和 FastAPI 开发统一受理接口，把一次请求拆成五路可独立恢复的任务。访问请求交付加密数据包；删除请求对每个数据源保存删除数量、剩余数量和执行回执，客户支持与合规人员可以直接查询完成进度和失败来源。

请求、任务、Outbox 和首条审计事件在 RDS PostgreSQL 同一事务提交。Outbox relay 将任务投递到 SQS，EKS worker 通过数据库租约决定执行资格，并同时延长 SQS visibility 与数据库 lease。区域连接器把成功率、HTTP 429 和 P95 延迟写入 Redis，Lua 脚本按租户和数据源原子更新 AIMD 并发窗口。Access 制品进入 S3，使用 KMS 加密和 SHA-256 metadata，API 返回五分钟下载链接；Delete 完成后反向查询剩余记录。PostgreSQL 16 的 100 租户基准完成 1000 个请求和 5000 个任务，1000 次幂等重放增加 0 次执行，100 条审计链全部通过重算。

## 需求如何从汉朔业务产生

汉朔设备侧已经存在两类真实工程入口：

1. AP 发现与配网工具扫描门店网段，写入 DHCP、网关、DNS、业务地址、端口、TLS 和自动发现参数，再通过 SSH 回读确认，单设备接入由约 5 分钟压缩到 40 秒。
2. 区域 API 采集工具分页读取门店、ESL 标识、最后心跳、电量、固件版本、刷新与闪灯次数、屏幕尺寸和产品型号。

隐私请求沿着同一数据域展开。一个账户标识会关联设备操作归属、带操作者标签的事件、刷新与升级任务的发起记录以及支持诊断工单。删除请求必须给出每个数据源的执行回执和剩余记录数，访问请求必须生成可交付、可校验、可过期的制品。零售商之间数据隔离，门店与区域服务存在不同吞吐上限，Worker 与区域网络都可能中断。

## 一次请求如何执行

### 1. 接口受理

客户端提交租户、API key、`Idempotency-Key`、请求类型、身份标识和数据源列表。Pydantic 完成字段与枚举校验，身份标识归一化后保存 SHA-256 key。数据库唯一约束为 `(tenant_id, idempotency_key)`；应用层预查处理普通重放，唯一约束处理并发竞态，相同命令返回原 request ID，不同命令返回 HTTP 409。

### 2. 原子创建任务

同一事务插入：

- 一条 request；
- 五条 source task；
- 五条 `privacy_task.ready` Outbox 事件；
- 一条 request-created Outbox 事件；
- 一条审计事件。

提交成功后，请求真相和待投递事件同时存在。API 在提交后发生连接中断时，客户端使用原幂等键取得同一 request ID。

### 3. Outbox 投递

多个 relay 使用 `FOR UPDATE SKIP LOCKED` 批量领取未发布行。SQS 确认消息后写入 `published_at`。relay 在发送后、数据库提交前退出会产生重复消息，消费端使用 task row 吸收重复。

### 4. Worker 领取与续租

消息只提供唤醒信号，PostgreSQL 任务行决定执行资格。原子更新检查 task ID、pending 状态或过期 lease，成功后写 owner、lease、attempt。处理期间每 60 秒延长 SQS visibility，数据库 lease 延长到 240 秒。Worker 失联后消息重新可见，lease 到期，另一个 Worker 领取并继续。

### 5. 区域数据源调用

连接器使用 HTTPS 与 Secrets Manager 服务令牌，发送租户 ID、request ID、`task_id:source` 幂等键和 subject hash。每个租户与区域数据源拥有独立 AIMD 窗口。健康窗口加 1，HTTP 429、失败或 P95 超过阈值时按比例减小。Redis Lua 在一条命令中读取、计算和保存窗口，并设置 TTL；多个 EKS Worker Pod 共享同一压力状态。

### 6. 结果核验

Access 汇总五路回执，按确定性对象键写入 S3。对象携带 SSE-KMS 和内容 SHA-256，API 返回 300 秒 presigned GET。Delete 对租户、subject hash 和 source 三个条件执行删除，再用相同条件查询活动记录；剩余记录数大于 0 时保存失败回执。

## 为什么同时用 PostgreSQL 和 SQS

PostgreSQL保存业务真相：请求状态、幂等键、任务租约、执行次数、回执和审计链。SQS保存投递状态：消息可见性、接收次数和 DLQ。两者通过 Outbox 连接。

只用 SQS 难以在创建请求时原子提交五个任务和审计证据，也难以让 API 精确查询每个数据源的执行状态。只轮询 PostgreSQL 会让大量 Worker 持续扫描任务索引。当前设计让 SQS负责唤醒与削峰，数据库行负责幂等执行和状态收敛。

## 消息在哪些位置会重复

三个窗口会产生重复：

1. relay 已发送 SQS，尚未写 `published_at`；
2. Worker 已完成数据源副作用，尚未写任务回执；
3. Worker 已写回执，尚未确认 SQS 消息。

数据库任务终态处理第 1 和第 3 个窗口。区域 API 使用 task/source 幂等键处理第 2 个窗口。Delete 使用可重入操作并执行 readback；Access 使用确定性读取和确定性 S3 对象键。重复消息命中 terminal task 时不增加 attempt，同时刷新父请求和缺失制品。

## 为什么 lease 和 visibility 都要续

SQS visibility 防止同一消息在长任务期间被另一个 Consumer 收到。数据库 lease 防止不同消息副本或人工重放同时获得任务执行权。两个时钟解决不同竞争面。

设置关系为：区域调用超时小于 heartbeat 周期，heartbeat 周期小于数据库 lease，数据库 lease 小于或接近 SQS visibility。生产配置采用 60 秒 heartbeat、240 秒数据库 lease、300 秒 visibility。Worker 每次心跳同时续两个状态。

## Redis AIMD 如何回答追问

固定并发适合容量稳定的单一区域服务。门店规模、区域网络和上游 API 限流会改变可接受并发。AIMD 使用直接反馈调节窗口：

```text
success and P95 <= target: cwnd = min(max, cwnd + 1)
failure or P95 > target:    cwnd = max(min, floor(cwnd * 0.5))
```

Redis key 为 `privacy:aimd:{tenant}:{source}`，另以 sorted set 保存 active task 与准入 lease 到期时间。准入 Lua 先清理过期 holder，再比较集合基数与窗口；窗口 Lua 原子读取当前值并写下一值，避免两个 Pod 同时基于旧值放大。TTL清理长时间没有流量的组合。窗口为 64 时的集成实验准入前 64 个 holder、阻断第 65 个，释放 1 个后立即允许新 holder。1000 操作的受控容量实验中，上游容量为 8，固定并发 32 产生 988 次首轮 429，AIMD 产生 25 次，下降 97.47%；成功吞吐从 28.53 提升至 432.11 operations/s。

## 数据库连接池故障如何回答

第一次将 100 个租户 Worker 全部并发执行时，SQLAlchemy 默认连接池只有 5 个常驻连接和 10 个 overflow，30 秒后出现 pool timeout。根因是 Worker 批次并发直接由租户数决定，超过数据库会话预算。

修复包含三项：

1. 显式配置 pool size 10、max overflow 20 和 10 秒获取超时；
2. 基准与 Worker 将并发限制为 10，为 Outbox relay、健康检查和父状态更新保留连接；
3. CloudWatch 结合 RDS 连接数、CPU、锁等待和 SQS oldest message 判断扩 Pod、扩数据库或优化查询。

修正后的 100 租户、1000 请求、5000 任务全部完成，任务尝试数保持 5000。

## 多租户隔离落在哪里

租户标识进入 request、task、source record、Outbox 和 audit event。外部资源查询同时过滤 resource ID 与 tenant ID，幂等唯一约束包含 tenant ID，S3 对象键按 tenant 分层，Redis AIMD key 也包含 tenant。隔离基准执行 10000 次跨租户请求，全部返回 404；100 次同租户控制查询全部返回 200。

## 审计链如何阻断并发分叉

每个租户使用单调 sequence。写入前获取 PostgreSQL advisory transaction lock，读取当前链头，再计算 payload hash、previous hash 和 event hash。`(tenant_id, sequence)` 唯一约束提供第二道并发约束。验证器按 sequence 从头重算，能够定位载荷修改、乱序和内部删除。100 个租户的链全部通过完整重算。

## S3 制品失败如何恢复

Access 最后一条任务回执提交后，聚合器写 S3 并将 URI 写回 request。S3 调用失败时，任务终态仍在数据库中，SQS 消息保留。重复消息命中 terminal task 后检查 `artifact_path`，缺失时重新聚合并覆盖同一确定性对象键，再写回 URI 并确认消息。LocalStack 集成验证上传 100 个对象，100 个均带 `aws:kms` 属性和 64 位 SHA-256 metadata。

## AWS 资源如何支撑生产

- EKS 分离 API 与 Worker Deployment，各自使用 Pod Identity；HPA、PDB、拓扑分散和滚动发布控制容量与中断。
- RDS PostgreSQL 16 Multi-AZ 保存业务状态，开启 TLS、备份、Performance Insights 和存储自动扩容。
- ElastiCache Redis 一主两副本，TLS、AUTH、KMS、自动故障转移和 7 天快照保存共享 AIMD 状态。
- SQS主队列使用长轮询、visibility heartbeat 和五次接收 DLQ；修复依赖后按 event ID redrive。
- S3开启公共访问阻断、SSE-KMS、versioning 和生命周期；KMS、Secrets Manager 与 CSI 管理数据密钥和连接凭据。
- CloudWatch 对 RDS CPU/存储、Redis CPU/eviction、SQS oldest message 和 DLQ visible message 建立告警。

## Benchmark 报数顺序

先给负载，再给结果：PostgreSQL 16，100 租户，50 个创建客户端，10 个 Worker 并发，1000 请求，每个请求 5 个任务，共 5000 个任务，Access 与 Delete 各占一半。

- 创建吞吐 63.03 requests/s，P50 715.5 ms，P95 1.272 s，P99 1.655 s。
- 任务处理 31.78 tasks/s，1000/1000 请求完成。
- 1000 次幂等回放全部返回原 ID，新增 attempt 为 0。
- 100/100 租户审计链通过重算。
- Worker 退出演练中 5/5 任务租约到期后被重新领取并完成。
- 500 条 SQS 消息全部确认，队列残留 0；100/100 S3 对象通过 KMS 与摘要检查。

## 个人承担

我负责需求拆解、数据模型、FastAPI 接口、事务边界、任务租约、Outbox、SQS 消费、Redis AIMD、S3/KMS 制品、审计链、Terraform、Helm、测试与基准。AP 配网和区域设备采集来自汉朔实习的实际设备域，隐私请求服务把这些数据源组织成可恢复的统一云服务。
