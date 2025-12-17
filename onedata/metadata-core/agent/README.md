# metadata-core

`metadata-core` 是独立的 Spring Boot 模块，用来承载统一元数据域的 CDC 消费、存储、搜索、血缘查询与事件推送能力。该模块参考 `metadataManagement.md` 中的设计，主要负责把 `datainjector` 推送到 `metadata.raw.*` Kafka 主题的 JSON/Avro 消息转化为 PostgreSQL 表中的实体，并对外提供 REST/SSE API。

## 能力概览

- **Kafka→PostgreSQL CDC**：`MetadataIngestionService` 通过 `@KafkaListener` 订阅 `metadata.ingestion.topics`，把 MetadataEnvelope（实体、属性、标签、血缘、质量指标）落入 `metadata_entity`, `metadata_attr`, `metadata_tag`, `metadata_lineage`, `metadata_event`, `metadata_quality`。
- **查询/搜索**：`MetadataSearchService` 支持按域、类型、平台、状态、关键字、标签等条件组合查询，返回分页列表。
- **详情与缓存**：`MetadataDetailService` 聚合实体、属性、最近事件、质量指标，使用 Redis Cache 缓存热点实体并在 ingestion 后自动失效。
- **血缘遍历**：`MetadataLineageService` 支持上下游多层遍历，最大深度由 `metadata.lineage.max-depth` 控制，适用于 Flink Job ↔ Kafka Topic ↔ ClickHouse 表等路径查看。
- **域统计与质量**：`DomainStatsService` 聚合域级活跃/异常实体数；`/entities/{id}/quality` 返回最近的质量快照。
- **变更推送**：`MetadataUpdatePublisher` 通过 SSE (`/v1/metadata/updates/stream`) 推送实时变更，前端可以订阅刷新发现页。

## 快速开始

```bash
cd metadata-core
mvn spring-boot:run
```

默认依赖以下基础设施，可在 `src/main/resources/application.yaml` 中覆盖：

- PostgreSQL：`jdbc:postgresql://localhost:5432/metadata`
- Kafka：`localhost:9092`
- Redis：`localhost:6379`

## 主要配置

```yaml
metadata:
  ingestion:
    topics:
      - metadata.raw.defi
      - metadata.raw.ch
      - metadata.raw.flink
  cache:
    ttl: 60s
  lineage:
    max-depth: 3
```

- **topics**：datainjector metadata role 推送的主题列表。
- **cache.ttl**：Redis 中实体详情缓存的 TTL。
- **lineage.max-depth**：血缘 API 返回的最大深度，避免一次性遍历过多节点。

## API 草图

| Method | Path | 说明 |
| --- | --- | --- |
| GET | `/v1/metadata/entities` | 列表搜索，支持 domain/type/platform/status/tags/keyword |
| GET | `/v1/metadata/entities/{id}` | 详情：基本信息、属性、标签、最近事件、质量 |
| GET | `/v1/metadata/entities/{id}/lineage?direction=up/down` | 血缘遍历 |
| GET | `/v1/metadata/entities/{id}/quality` | 最新质量指标 |
| GET | `/v1/metadata/domains/{domain}/stats` | 域级实体统计 |
| GET | `/v1/metadata/updates/stream` | SSE 实时变更流 |

## 下一步

- 在 datainjector 中补充 `metadata.*` role 并输出符合 `MetadataEnvelope` 的消息。
- 扩展 `MetadataSearchService` 的标签查询为批量 SQL，优化 N+1。
- 接入 ClickHouse/Paimon 派生表，为质量趋势、历史快照提供查询接口。
- 前端 `MetadataDiscovery` 页面调用以上 API，实现搜索/详情/血缘视图。
