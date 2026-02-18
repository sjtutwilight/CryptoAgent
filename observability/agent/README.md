# DataPlatform 可观测体系

## 概述

本项目实现了完整的可观测性体系，覆盖以下核心组件：
- **Worker (数据接入层)**: Go服务，负责数据采集
- **Backend (API服务)**: Spring Boot服务，提供RESTful API
- **Kafka**: 消息队列
- **Flink**: 流处理引擎
- **ClickHouse**: 时序数据库

## 架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          可观测性组件                                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │  Prometheus  │    │    Loki      │    │   Grafana    │              │
│  │   (指标)     │    │   (日志)     │    │   (可视化)   │              │
│  │   :9090      │    │   :3100      │    │   :3000      │              │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘              │
│         │                   │                   │                       │
│         │   scrape          │   push            │   query               │
│         ▼                   ▼                   ▼                       │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │                     Promtail (日志采集)                        │      │
│  └──────────────────────────────────────────────────────────────┘      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
            ┌───────────┐   ┌───────────┐   ┌───────────┐
            │  Worker   │   │  Backend  │   │  Kafka    │
            │  :9100    │   │  :8088    │   │  :9308    │
            └───────────┘   └───────────┘   └───────────┘
                    │               │               │
                    ▼               ▼               ▼
            ┌───────────┐   ┌───────────┐   ┌───────────┐
            │  Flink    │   │ClickHouse │   │           │
            │  :9249    │   │  :9363    │   │           │
            └───────────┘   └───────────┘   └───────────┘
```

## 组件端口

| 组件 | 端口 | 说明 |
|------|------|------|
| Grafana | 3000 | 可视化仪表盘 (admin/admin) |
| Prometheus | 9090 | 指标存储与查询 |
| Loki | 3100 | 日志聚合 |
| Worker Metrics | 9100 | Worker Prometheus端点 |
| Backend Metrics | 8088/api/actuator/prometheus | Backend Prometheus端点 |
| Kafka Exporter | 9308 | Kafka指标导出 |
| Flink JobManager | 9249 | Flink JobManager指标 |
| Flink TaskManager | 9250 | Flink TaskManager指标 |
| ClickHouse | 9363 | ClickHouse指标 |

## 快速开始

### 1. 启动可观测性组件

```bash
docker-compose up -d prometheus loki grafana promtail kafka-exporter
```

### 2. 访问Grafana

打开 http://localhost:3000，使用 admin/admin 登录。

### 3. 查看预置仪表盘

- **Worker 可观测性**: 消息处理、队列状态、WebSocket连接、完整性模块
- **Backend 可观测性**: HTTP请求、JVM指标、数据库连接池
- **Kafka 可观测性**: Topic指标、Consumer Lag
- **Flink 可观测性**: Checkpoint、背压、资源使用
- **ClickHouse 可观测性**: 写入延迟、Merge队列、查询性能

## 指标体系

### Worker 指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `worker_messages_received_total` | Counter | 接收的消息总数 |
| `worker_messages_processed_total` | Counter | 处理完成的消息总数 |
| `worker_messages_sent_total` | Counter | 发送到Kafka的消息总数 |
| `worker_messages_processing_latency_seconds` | Histogram | 消息处理延迟 |
| `worker_queue_size` | Gauge | 当前队列深度 |
| `worker_queue_capacity` | Gauge | 队列容量 |
| `worker_websocket_connections` | Gauge | WebSocket连接数 |
| `worker_websocket_reconnects_total` | Counter | 重连次数 |
| `worker_integrity_gaps_total` | Counter | 序列号Gap数 |
| `worker_integrity_backfill_sessions_inflight` | Gauge | Backfill会话进行中数量 |
| `worker_integrity_backfill_result_total` | Counter | Backfill结果计数（success/fail/timeout） |
| `worker_integrity_backfill_pending_duration_seconds` | Histogram | Backfill pending 持续时长 |
| `worker_integrity_backfill_schedule_dedup_total` | Counter | Backfill去重命中次数 |
| `worker_integrity_buffer_size` | Gauge | 乱序缓冲区大小 |

### Backend 指标

Spring Boot Actuator自动暴露以下指标：

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `http_server_requests_seconds` | Histogram | HTTP请求延迟 |
| `jvm_memory_used_bytes` | Gauge | JVM内存使用 |
| `jvm_gc_pause_seconds` | Summary | GC暂停时间 |
| `jvm_threads_live_threads` | Gauge | 活跃线程数 |
| `hikaricp_connections_active` | Gauge | 活跃数据库连接 |
| `hikaricp_connections_pending` | Gauge | 等待中的连接请求 |

## 告警规则

告警规则位于 `prometheus/rules/alerts.yml`，覆盖以下场景：

### Kafka 告警
- `KafkaConsumerLagHigh`: Consumer Lag > 10000
- `KafkaConsumerLagCritical`: Consumer Lag > 100000
- `KafkaBrokerDown`: Broker离线

### Flink 告警
- `FlinkCheckpointFailed`: Checkpoint失败
- `FlinkCheckpointDurationHigh`: Checkpoint耗时 > 60s
- `FlinkTaskManagerMemoryHigh`: 内存使用 > 90%
- `FlinkBackpressureHigh`: 背压严重

### ClickHouse 告警
- `ClickHouseInsertLatencyHigh`: 写入延迟过高
- `ClickHouseMergeBacklog`: Merge队列积压
- `ClickHouseMemoryHigh`: 内存使用 > 8GB

### Worker 告警
- `WorkerDown`: Worker服务离线
- `WorkerErrorRateHigh`: 错误率 > 5%
- `WorkerQueueBacklog`: 队列使用率 > 80%
- `WorkerWebSocketDisconnected`: WebSocket断连

### Backend 告警
- `BackendDown`: Backend服务离线
- `BackendLatencyHigh`: P95延迟 > 2s
- `BackendErrorRateHigh`: 5xx错误率 > 5%
- `BackendHeapMemoryHigh`: 堆内存 > 85%

## 日志采集

Promtail配置支持以下日志源：

1. **Docker容器日志**: 自动采集所有容器日志
2. **Flink应用日志**: `/var/log/app/flink/*.log`
3. **Worker日志**: `/var/log/app/worker/*.log`
4. **Backend日志**: `/var/log/app/backend/*.log`
5. **JSON格式日志**: `/var/log/app/*.json`

### 日志标签

- `job`: 日志来源 (docker/flink/worker/backend)
- `component`: 组件名称
- `level`: 日志级别
- `role_id`: Worker角色ID (仅Worker日志)

## 配置说明

### Worker Metrics配置

在 `configs/config.yaml` 中配置：

```yaml
metrics:
  enabled: true    # 是否启用metrics暴露
  port: 9100       # metrics HTTP端口
  path: "/metrics" # metrics路径
```

### Backend Metrics配置

在 `application.yml` 中已配置：

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

## 目录结构

```
observability/
├── README.md                    # 本文档
├── prometheus/
│   ├── prometheus.yml           # Prometheus配置
│   └── rules/
│       └── alerts.yml           # 告警规则
├── promtail/
│   └── config.yml               # Promtail日志采集配置
├── clickhouse/
│   ├── prometheus.xml           # ClickHouse Prometheus配置
│   └── sql_exporter.yml         # SQL Exporter配置
├── provisioning/
│   ├── dashboards/
│   │   ├── dashboard.yml        # Dashboard provider配置
│   │   ├── worker-observability-dashboard.json
│   │   ├── backend-observability-dashboard.json
│   │   ├── kafka-observability-dashboard.json
│   │   ├── flink-observability-dashboard.json
│   │   └── clickhouse-observability-dashboard.json
│   └── datasources/
│       └── datasource.yml       # 数据源配置
└── 演练.md                       # 故障演练手册
```

## 故障排查

### 1. Prometheus无法抓取目标

检查目标是否可达：
```bash
curl http://localhost:9100/metrics  # Worker
curl http://localhost:8088/api/actuator/prometheus  # Backend
```

### 2. Grafana无数据

1. 检查Prometheus数据源配置
2. 确认Prometheus能正常抓取目标
3. 检查时间范围是否正确

### 3. 日志未采集

1. 检查Promtail配置中的路径是否正确
2. 确认日志文件存在且有写入
3. 查看Promtail日志：`docker logs obs-promtail`

## 扩展

### 添加新的监控目标

1. 在 `prometheus/prometheus.yml` 添加新的 scrape_config
2. 创建对应的Grafana Dashboard
3. 添加相关告警规则

### 自定义告警

1. 在 `prometheus/rules/alerts.yml` 添加规则
2. 重载Prometheus配置：
```bash
curl -X POST http://localhost:9090/-/reload
```

## 参考文档

- [Prometheus官方文档](https://prometheus.io/docs/)
- [Grafana官方文档](https://grafana.com/docs/)
- [Loki官方文档](https://grafana.com/docs/loki/)
- [Spring Boot Actuator](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html)


