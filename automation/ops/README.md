# Automation Ops

统一操作入口：`./tool/ops.sh <domain:action> [args...]`

## Common Examples

Role lifecycle:

```bash
./tool/ops.sh role:start binance-spot-link-kline-batch
./tool/ops.sh role:stop role-a role-b
./tool/ops.sh role:stop all
./tool/ops.sh role:alive_list
./tool/ops.sh role:task binance-spot-link-kline-batch
```

Init:

```bash
./tool/ops.sh init:schema
./tool/ops.sh init:all --skip-data
```

SQLite metadata:

```bash
./tool/ops.sh sqlite:query list-sources
./tool/ops.sh sqlite:query show binance.spot.kline.btcusdt.5m
./tool/ops.sh sqlite:clean binance.spot.kline.btcusdt.5m --confirm
```

StarRocks:

```bash
./tool/ops.sh starrocks:query --mode count
```

HTTP:

```bash
./tool/ops.sh http:get https://api.binance.com/api/v3/ping
```

Flink:

```bash
# Default REST: http://localhost:8081 (override with FLINK_REST_URL)
./tool/ops.sh flink:list
./tool/ops.sh flink:upload
./tool/ops.sh flink:run --entry-class com.twilight.aggregator.KlineSignalJob
./tool/ops.sh flink:job kline
./tool/ops.sh flink:job perp
./tool/ops.sh flink:status
./tool/ops.sh flink:cancel
./tool/ops.sh flink:cancel all
./tool/ops.sh flink:cancel kline
```

## Notes

- `role:start|stop` 支持 `--api` / `--container` / `--token` 参数。
- 默认输出人类可读；需要 JSON 使用 `--output-json`。
- `flink:upload` / `flink:run` 默认使用最新构建的 jar。
- `flink:status` 默认展示全部 job（可传 job id 查看单个）。
- `flink:cancel` 默认取消全部运行中的 job；关键词会按 Flink job 名称匹配当前任务。
