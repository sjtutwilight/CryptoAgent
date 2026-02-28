# Codex 日志-链路联动验证记录

## 目标

验证值班链路可完成 `日志 -> Trace -> 日志` 的双向跳转前置配置。

## 配置检查结果

### 1) Grafana 数据源联动配置

- 文件：`observability/provisioning/datasources/datasource.yml`
- 关键项：
  - `Loki.derivedFields[].datasourceUid = tempo`
  - `Tempo.uid = tempo`
  - `Tempo.jsonData.tracesToLogsV2.datasourceUid = loki`

本地检查命令：

```bash
rg -n "datasourceUid: tempo|uid: tempo|tracesToLogsV2|matcherRegex" \
  observability/provisioning/datasources/datasource.yml
```

检查输出（节选）：

- `datasourceUid: tempo`
- `uid: tempo`
- `tracesToLogsV2`

### 2) 监控抓取联动配置

- 文件：`observability/prometheus/prometheus.yml`
- 关键项：
  - `job_name: otel-collector`
  - `job_name: tempo`
  - `job_name: codex-otel-metrics`

本地检查命令：

```bash
rg -n "OTel / Trace 监控|otel-collector|tempo|codex-otel-metrics" \
  observability/prometheus/prometheus.yml
```

## 结论

- 配置层面已满足日志-Trace 联动所需的 datasource 关系与指标抓取关系。
- 在 Grafana 组件启动后，可直接通过 Loki `trace_id` 跳转 Tempo，并通过 Tempo `tracesToLogsV2` 回查 Loki。
