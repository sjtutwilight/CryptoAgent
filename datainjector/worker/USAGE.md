# datainjector/worker 使用说明

## 模块定位
`worker` 是 DataInjector 的执行模块：按角色配置采集数据、处理数据并输出到下游（Kafka/文件/控制台）。

## 模块能力
- 配置驱动角色运行：通过 `role` 定义触发方式、采集调用、处理链和下沉方式，统一治理不同数据接入任务。
- 多触发模式：支持 `single`（持续流触发）、`polling`（定时触发）、`kafka_command`（Kafka 指令触发）。
- 多类型数据采集：支持 WebSocket/HTTP/SDK，以及 Kafka/Postgres/ClickHouse 元数据采集。
- 数据完整性与补数闭环：支持 `integrity`/`missing_detector`，可做序列缺口检测、补数调度和结果回执。
- 多下沉目标：支持 `kafka`、`file`、`console`；`orderbook` 可按 diff/snapshot 双 topic 路由输出。
- 批量分页落盘：`batch_file` 任务支持分页拉取、断点游标与 `manifest` 产物。
- 在线治理 API：提供角色查询、校验、解析、热应用、停用接口。
- 可观测性：暴露 `/metrics`，输出结构化日志事件，支持 tracing 与任务状态上报（`tasks.status`）。

## 关键访问路径
### 路径 A：启动即加载角色（最常用）
1. 准备基础配置：`configs/base.yaml`。
2. 选择角色工件：如 `configs/aave/roles_aave_full_stable.json`。
3. 设置 Kafka 连接来源（环境变量或角色内 brokers）。
4. 启动 worker。

```bash
cd datainjector/worker
# 中文注释：Kafka 地址建议走环境变量，避免写死
export KAFKA_BROKERS="127.0.0.1:9092"
go run ./cmd/worker --config ./configs/base.yaml --roles ./configs/aave/roles_aave_full_stable.json
```

### 路径 B：空载启动 + 控制 API 在线发布
1. 先空载启动 worker（不传 `--roles`）。
2. 调用 `/api/roles/validate` 校验角色。
3. 调用 `/api/roles/apply` 应用角色。
4. 用 `/api/roles` 查看运行中角色，用 `/api/roles/stop` 停止角色。

```bash
# 中文注释：若配置了 api_server.token，请增加 -H 'X-Worker-Token: <token>'
curl -s http://127.0.0.1:8090/healthz
curl -s http://127.0.0.1:8090/api/roles
```

常用接口：
- `GET /api/roles`：查询运行中角色。
- `POST /api/roles/validate`：校验角色配置合法性。
- `POST /api/roles/resolve`：输出模板展开后的最终角色配置。
- `POST /api/roles/apply`：热应用角色（增/改/删）。
- `POST /api/roles/stop`：停止指定角色。

### 路径 C：AAVE 微观结构默认链路
1. 使用 `configs/aave/roles_aave_full_stable.json` 启动。
2. 订单簿输出为双 topic：`*.orderbook.diff` + `*.orderbook.snapshot`。
3. `aggtrades` 独立输出到 `*.aggtrades`。
4. 需要回滚时切换为 `configs/aave/roles_aave_full_stable_file_rollback.json`。

## 配置与脚本说明
### 配置文件
- `configs/base.yaml`
  - 运行时基础配置：`status_reporter`、`metrics`、`api_server`、`logging`、`tracing`、`datasources`。
- `configs/aave/roles_aave_full_stable.json`
  - AAVE 稳定发布工件（Kafka-first，含 diff/snapshot 路由约定）。
- `configs/aave/roles_aave_full_stable_file_rollback.json`
  - AAVE 回滚工件。
- `configs/config.yaml`
  - 角色注册中心样例（线下管理用），不是默认运行时加载入口。

### 常用脚本
- `tools/quality_gate.sh`
  - 质量门禁统一入口（`golangci-lint + go-arch-lint`）。
- `tools/quality_gate_regression.sh`
  - 门禁最小回归测试（成功/新增 lint 违规/新增架构违规）。
- `tools/worker_observability_cutover_gate.sh`
  - 可观测性切换门禁（规则、Dashboard、Promtail、运行时检查）。
- `tools/nf01_nf03_gray_drill.sh`
  - NF-01~NF-03 灰度演练测试入口。
- `tools/recording_to_ods.py`
  - 将 worker 录制的 `jsonl` 转换为 ODS 目录与元数据。
- `tools/mock_worker_observability_server.py`
  - 本地观测联调的 mock 服务入口。

## 项目产物
| 产物 | 产生时机 | 作用 |
| --- | --- | --- |
| Kafka topic 数据（如 `spot/perp.orderbook.diff`、`*.snapshot`、`*.aggtrades`） | 角色 sink 为 `kafka` 且角色运行中 | 为下游实时链路提供标准化流数据 |
| `runtime/data/...` 文件数据 | sink 为 `file` 或启用录制/观测输出 | 本地调试、留样、回放 |
| `manifest.json`（批量任务） | `batch_file` 任务完成后 | 记录任务级文件清单、记录数、校验信息 |
| `.cursor.json`（批量任务） | `batch_file` 任务执行中断或分页进行中 | 断点续跑，避免整批重拉 |
| `runtime/data/backfill_compensation_<role_id>.json` | 开启 backfill 持久化补偿且入队失败/重放 | 防止补数指令静默丢失 |
| `runtime/quality_gate/*` | 运行 `tools/quality_gate.sh` | 输出当前违规、差异和汇总结果 |
| `/metrics` 指标暴露 | `metrics.enabled=true` 且进程运行中 | Prometheus 抓取与告警 |
| `tasks.status` 事件 | `status_reporter.enabled=true` 且配置 brokers | 向控制面回传任务阶段/终态 |
| ODS 目录与 `metadata.json/manifest.json` | 执行 `tools/recording_to_ods.py` | 供离线分析、数据集登记 |

## 反模式（不要这样用）
- 将 `configs/config.yaml` 当成默认启动配置直接依赖。
  - 运行时主入口是 `--config` + `--roles`，角色文件需显式传入。
- 使用 `kafka` sink 或 `kafka_command` emitter 时未提供 brokers 来源。
  - 会导致初始化失败或无法消费任务。
- 在共享环境硬编码 Kafka 地址（如 `localhost:9092`、`kafka:29092`）。
  - 会造成环境迁移失败，建议统一走环境变量。
- 给 role 配置 `integrity/missing_detector`，同时把 `queue.mode` 设为 `none`。
  - 该组合不受支持，角色构建会失败。
- 配置了 `api_server.token`，但调用 API 不带 `X-Worker-Token`。
  - 请求会返回 `401`。
