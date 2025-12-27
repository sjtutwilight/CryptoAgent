# Tool Entrypoints

统一入口脚本，提供 infra/编排/数据初始化/测试/观测能力。

## 设计原则

**入口脚本职责：**
- 提供统一的命令行界面
- 参数解析与验证
- 调用具体实现模块（不实现具体功能）

**具体实现位置：**
- 测试逻辑 → `automation/test/probes/` 和 `automation/test/scenarios/`
- 数据处理 → `process/batch/spark/jobs/`
- 编排逻辑 → `automation/orchestration/`

## Scripts

- `tool/orchestration.sh`   - 编排与开发场景入口（调用 docker-compose、服务启停脚本）
- `tool/data.sh`            - 数据初始化与批处理作业入口（调用 init-load 与 Spark 作业）
- `tool/test.sh`            - 测试入口（调用 automation/test 下的 Scenario）

## Examples
```bash
./tool/data.sh spark:upload-test-data
./tool/data.sh spark:token-holders --input-path s3a://paimon-warehouse/test-data/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/
./tool/test.sh scenario:spark_token_holders --config-json '{"chain_id":1,"token_address":"0x514910771af9ca656af840dff83e8264ecf986ca"}'
```
