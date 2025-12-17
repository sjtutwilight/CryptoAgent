构建覆盖 EVM DeFi 协议、ClickHouse/Flink/Kafka/Paimon 等资产的统一元数据管理，以支撑采集、存储、查询、发现三个闭环；采集侧复用现有 Go Worker，核心+发现页面做新模块。
元数据内容包含结构 schema、运行配置、血缘、质量状态、标签；对接现有实时加工链路（Flink/ClickHouse）与应用层（Spring Boot API + React 前端）。
现有能力评估

采集层：Role/Emitter/Handler/Sink 框架已支持多协议拉流、责任链处理与 Kafka 下沉，并内建补数和状态上报机制，可直接承载新的元数据任务（datainjector/worker/internal/role/role.go (lines 24-520), datainjector/worker/internal/config/config.go (lines 11-117), datainjector/worker/configs/config.yaml (lines 7-195)）。
实时处理层：Flink 标准化流水线已经在 ProcessEvent→元数据增强→指标广播→ClickHouse 的模式上投入生产，可复用 Redis 缓存与广播机制来服务 metadata enrichment 与质量监控（aggregator/src/main/java/com/twilight/aggregator/AccountBalanceJob.java (lines 37-189), aggregator/src/main/java/com/twilight/aggregator/process/common/EventEnrichmentMap.java (lines 21-166), aggregator/src/main/java/com/twilight/aggregator/process/common/RedisTokenMetricsBroadcaster.java (lines 20-114)）。
数据湖仓：Flink SQL 作业已把 Kafka 事件落到 Paimon/MinIO，并具备建模事实表的经验，可扩展到元数据快照和变更日志（aggregator/src/main/java/com/twilight/aggregator/FlinkSqlJob.java (lines 1-217)）。
应用层：Spring Boot 控制器+Repository 模式已经串联 PostgreSQL（业务维表）与 ClickHouse（指标表），并通过 REST/WebSocket 服务前端；可在该模块内新增 Metadata API（backend/src/main/java/com/twilight/backend/controller/NewAnalyticsController.java (lines 33-200), backend/src/main/java/com/twilight/backend/repository/impl/TokenRepositoryImpl.java (lines 31-200)）。
前端：React Router + MUI 布局已存在多页签导航，易于增加“Metadata Discovery”页面并挂载上下文（frontend/src/App.js (lines 1-110)）。
元数据采集方案

统一策略：在 datainjector 中新增 metadata.* 角色，按域配置 caller、handler 和 sink，将采集到的 JSON/Avro 推到 Kafka（metadata.raw.<domain>），由 metadata-core 统一消费；继承现有 backfill、限流、状态上报能力以保证高可用。
EVM DeFi 协议：使用 native_call WebSocket/HTTP 触发器拉取合约 ABI、池子组成、路由参数；定制 handler 将 ABI/事件映射转换为标准 MetadataEntity 并附上链、协议、标签、source block，高频流量可经 single emitter 监听链上注册事件。
ClickHouse：配置 polling emitter 走 HTTP SQL 接口 (system.tables, system.columns)；handler 解析列类型、主键、TTL、物化视图信息；sink 写入 metadata.raw.ch.
Flink：利用 native_call HTTP 请求 JobManager REST (/jobs, /jobs/{id}) 和 Savepoint API；handler 生成 Job→Operator→Topic/表 的血缘片段，并标记资源槽数、checkpoint 间隔。
Kafka：实现 sdk_call（重用 Go Kafka 客户端）定期拉取 topic/ACL/schema registry 元数据；按集群维度合并 partition 配置、保留策略、生产者/消费者 lag。
Paimon：借助现有 Flink SQL 作业在批量导入后，同步运行 DESCRIBE 和 SHOW SNAPSHOTS 任务；也可直接扫描 manifest 目录，通过 handler 产出 schema 版本与主键变更。
统一落库：所有 handler 产出的消息都写入 metadata.raw Kafka，metadata-core 以 CDC 方式消费，落表并触发索引更新。
元数据核心与存储设计

存储层次：PostgreSQL 存主实体/属性（强一致查询）、Paimon 存长历史与事件流（低成本归档）、Redis 存热缓存给 Flink/前端；必要时在 ClickHouse 建列存派生表便于指标分析。
逻辑模型：metadata_entity(id, type, name, domain, platform, locator, version, status), metadata_attr(entity_id, key, value_json, level), metadata_lineage(upstream_id, downstream_id, relation_type, confidence), metadata_tag(entity_id, tag), metadata_event(entity_id, change_type, payload, occurred_at)；EVM 协议额外扩展 contract_address, abi_sha, protocol, chain_id，平台类实体带 cluster, db, topic, job_id 等。
服务模块：metadata-ingestor（Spring Boot 消费 Kafka 并写库）、metadata-api（REST + WebSocket/Server-Sent Events），metadata-search（Elasticsearch/PG trigram 提供搜索），lineage-builder（异步 Job 做 graph 合并）。
API 设计：遵循 /v1 命名空间，提供 GET /metadata/entities（搜索+过滤）、GET /metadata/entities/{id}（详情+属性+最近事件）、GET /metadata/entities/{id}/lineage?direction=up/down、GET /metadata/domains/{domain}/stats；对于实时变更提供 /topic/metadata/updates WebSocket 通知，风格与现有 analytics API 对齐（backend/src/main/java/com/twilight/backend/controller/NewAnalyticsController.java (lines 49-190)）。
质量监控：利用 Flink job 输出 metadata_quality 指标（完整率、延迟、版本漂移），写入 ClickHouse 方便后台页面展示；同样可在 metadata-core 提供 /metadata/entities/{id}/quality。
元数据发现页面

导航：在 App.js 中新增 /metadata NavLink 并挂载新的 MetadataDiscovery 组件，与现有 Dashboard/Analytics 平行（frontend/src/App.js (lines 58-110)）。
页面布局：左侧为搜索/过滤（按域、类型、链、标签），中间列表展示实体卡片（名称、平台、更新时间、质量状态），右侧展示详情/血缘小窗；详情页包含基本信息、schema/ABI、上下游、运行状态、示例指标（从 ClickHouse 或 Redis 拉取）。
数据获取：统一通过 metadata API，列表页使用分页+排序，详情页在切换时并行请求 entity、attributes、lineage、质量；Subscribe SSE/WebSocket 以实时刷新状态；前端沿用 MUI Table/Card + Recharts (用于 lineage 迷你图)。
交互：支持复制资源定位符（URL、SQL、RPC path）、跳转到 ClickHouse/Flinnk 面板、触发元数据校验任务；提供“收藏/订阅”功能：前端调用 POST /metadata/entities/{id}/watchers。
端到端流程示例

EVM 新协议：datainjector metadata-role 监听链上 Factory 事件→写入 metadata.raw.defi→metadata-core 将合约 ABI/Pair 信息写入实体表并广播→Flink EventEnrichmentMap 改为拉取 metadata-core API/Redis 缓存，保证事件解析（aggregator/src/main/java/com/twilight/aggregator/process/common/EventEnrichmentMap.java (lines 66-166)）能立刻识别。
ClickHouse 表变更：metadata-role 调用 system tables→检测列 hash 变化→写实体版本 & lineage（表→Flink Job）→Quality 指标写 ClickHouse→前端详情展示差异；TokenRepository 也可读取 metadata-core 获取表描述而不再手写 SQL（backend/src/main/java/com/twilight/backend/repository/impl/TokenRepositoryImpl.java (lines 70-200)）。
Flink 作业：metadata-role 调 Flink REST→生成 Job→Kafka Topic→ClickHouse 表链路→metadata-core 提供 /lineage 展示→Flink Sql Job 也把 schema 快照写 Paimon（aggregator/src/main/java/com/twilight/aggregator/FlinkSqlJob.java (lines 189-217)）供历史分析。
Kafka/Paimon：周期性 metadata-role 读取 topic/warehouse 指标→metadata-core 计算保留策略、分区健康→UI 标记是否超配/过期，并可跳链路至处理作业/消费 API。
MVP里程碑与风险

里程碑：① 定义 metadata_entity 模型与 Kafka schema，完成 datainjector role POC（1-2 周）；② 落地 metadata-core 服务（API + ingestion + lineage builder，2-3 周）；③ 上线前端发现页面 MVP（列表 + 详情 + 血缘视图，1-2 周）；④ 关联现有 Flink/Backend 使用 metadata-core（EventEnrichment/TokenRepository 加缓存，1 周）。
风险/对策：Kafka Admin/Flink REST 需要额外鉴权（在 metadata-role 中集成 credential 管理）；EVM ABI 量大需要去重，可借助 ABI SHA + Redis 缓存；全局搜索需补索引，可先用 PostgreSQL trigram 简化；发现页血缘可先返回树状 JSON，图可后续优化。
下一步建议

确认 metadata-core 技术栈（复用 backend Spring Boot 还是独立模块）并建立数据库 schema。
编写 datainjector metadata role 配置样例与 handler skeleton，验证 Kafka→Postgres 链路。
定义 REST/OpenAPI 规约与前端数据契约，启动 MetadataDiscovery 页面骨架开发。

当前进展

- 新增 `metadata_kafka` caller（datainjector/worker/internal/caller/metadata_kafka.go），可直接通过 Kafka Admin 协议拉取 topic/partition 元数据，输出统一 JSON 给 metadata raw topic。
- 在 `datainjector/worker/configs/config.yaml` 中追加 `metadata-kafka` role 示例，复用 polling emitter 和 kafka sink，即可定时写入 `metadata.raw.kafka`。
- 新增 `metadata_postgres` 和 `metadata_clickhouse` caller，分别基于信息_schema / system.* 视图采集表与字段信息，并在默认配置中提供 role 示例，方便将 Postgres/ClickHouse 元数据推送到 `metadata.raw.postgres` 与 `metadata.raw.clickhouse`。
- Postgres/ClickHouse caller 支持 `*_env` 参数与 DSN 解析，可直接读取环境变量中的连接串/密码，同时自动解析数据库名，避免启动期必须访问数据库。
- 新增 `metadata_envelope` handler，将原始 metadata payload 包装为 metadata-core 所需的 Envelope 结构（实体 + 属性 + 可选 tags/lineage/quality），并基于 cluster/database/schema/table 生成稳定 UUID 及 locator，保证 ingestion service 能直接消费。

## 元数据建模
-- Core entity catalog
CREATE TABLE IF NOT EXISTS metadata_entity (
    id UUID PRIMARY KEY,
    type VARCHAR(64) NOT NULL,
    name VARCHAR(256) NOT NULL,
    domain VARCHAR(64),
    platform VARCHAR(64),
    locator VARCHAR(512),
    version VARCHAR(64),
    status VARCHAR(32) DEFAULT 'UNKNOWN',
    protocol VARCHAR(64),
    chain_id VARCHAR(64),
    contract_address VARCHAR(128),
    cluster VARCHAR(64),
    db_name VARCHAR(128),
    topic VARCHAR(256),
    job_id VARCHAR(128),
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metadata_entity_domain ON metadata_entity (domain);
CREATE INDEX IF NOT EXISTS idx_metadata_entity_type ON metadata_entity (type);
CREATE INDEX IF NOT EXISTS idx_metadata_entity_status ON metadata_entity (status);
CREATE INDEX IF NOT EXISTS idx_metadata_entity_updated_at ON metadata_entity (updated_at DESC);

-- Entity attributes (JSON payload stored as text to stay engine-agnostic)
CREATE TABLE IF NOT EXISTS metadata_attr (
    id BIGSERIAL PRIMARY KEY,
    entity_id UUID NOT NULL REFERENCES metadata_entity(id) ON DELETE CASCADE,
    key VARCHAR(128) NOT NULL,
    value_json TEXT,
    level VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metadata_attr_entity ON metadata_attr (entity_id);
CREATE INDEX IF NOT EXISTS idx_metadata_attr_key ON metadata_attr (key);

-- Tags
CREATE TABLE IF NOT EXISTS metadata_tag (
    id BIGSERIAL PRIMARY KEY,
    entity_id UUID NOT NULL REFERENCES metadata_entity(id) ON DELETE CASCADE,
    tag VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_tag_unique ON metadata_tag (entity_id, tag);

-- Lineage edges
CREATE TABLE IF NOT EXISTS metadata_lineage (
    id BIGSERIAL PRIMARY KEY,
    upstream_id UUID NOT NULL REFERENCES metadata_entity(id) ON DELETE CASCADE,
    downstream_id UUID NOT NULL REFERENCES metadata_entity(id) ON DELETE CASCADE,
    relation_type VARCHAR(64),
    confidence DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metadata_lineage_upstream ON metadata_lineage (upstream_id);
CREATE INDEX IF NOT EXISTS idx_metadata_lineage_downstream ON metadata_lineage (downstream_id);

-- Change events (for audit + SSE payloads)
CREATE TABLE IF NOT EXISTS metadata_event (
    id BIGSERIAL PRIMARY KEY,
    entity_id UUID NOT NULL REFERENCES metadata_entity(id) ON DELETE CASCADE,
    change_type VARCHAR(32) NOT NULL,
    payload TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metadata_event_entity_time ON metadata_event (entity_id, occurred_at DESC);

-- Quality snapshots
CREATE TABLE IF NOT EXISTS metadata_quality (
    id BIGSERIAL PRIMARY KEY,
    entity_id UUID NOT NULL REFERENCES metadata_entity(id) ON DELETE CASCADE,
    completeness DOUBLE PRECISION,
    freshness DOUBLE PRECISION,
    schema_drift DOUBLE PRECISION,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metadata_quality_entity_time ON metadata_quality (entity_id, collected_at DESC);
