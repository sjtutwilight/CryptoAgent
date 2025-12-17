# Mock数据源服务

## 概述

Mock数据源服务是一个用Go 1.20开发的模拟以太坊节点服务，用于测试加密货币数据接入系统。它提供了WebSocket和HTTP接口，模拟真实的以太坊节点行为，并支持故障注入来测试系统的容错能力。

## 功能特性

### 核心功能
- **WebSocket服务**: 支持`eth_subscribe newHeads`订阅，定期推送新区块头
- **HTTP服务**: 提供`eth_getBlockByNumber`、`eth_blockNumber`等JSON-RPC接口
- **数据生成器**: 生成模拟的以太坊区块头数据
- **故障注入器**: 模拟各种故障情况来测试系统容错能力

### 故障注入类型
- **HTTP故障**:
  - 随机请求失败 (400, 404)
  - 限流错误 (429)
  - 服务器错误 (500, 503)
- **WebSocket故障**:
  - 连接断开模拟
  - 数据丢失模拟
  - 心跳异常模拟
- **链重组故障**:
  - 模拟区块链重组(Chain Reorg)
  - 支持配置回退区块数范围
  - 模拟真实的分叉场景

## 快速开始

### 1. 安装依赖
```bash
go mod tidy
```

### 2. 运行服务
```bash
go run main.go
```

或者使用自定义配置文件：
```bash
go run main.go /path/to/config.yaml
```

### 3. 服务端点

服务启动后，可以通过以下端点访问：

- **WebSocket**: `ws://localhost:8080/ws`
- **HTTP JSON-RPC**: `http://localhost:8080/`
- **健康检查**: `http://localhost:8080/health`
- **故障注入统计**: `http://localhost:8080/fault/stats`

## 使用示例

### WebSocket订阅示例

```javascript
const ws = new WebSocket('ws://localhost:8080/ws');

// 订阅新区块头
ws.onopen = function() {
    const subscribeRequest = {
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newHeads"],
        "jsonrpc": "2.0"
    };
    ws.send(JSON.stringify(subscribeRequest));
};

// 接收新区块头通知
ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log('收到消息:', data);
};
```

### HTTP请求示例

```bash
# 获取当前区块号
curl -X POST http://localhost:8080/ \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1,
    "method": "eth_blockNumber",
    "params": [],
    "jsonrpc": "2.0"
  }'

# 获取指定区块
curl -X POST http://localhost:8080/ \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1,
    "method": "eth_getBlockByNumber",
    "params": ["latest", false],
    "jsonrpc": "2.0"
  }'
```

## 配置文件

配置文件位于 `configs/config.yaml`，可以自定义以下参数：

```yaml
server:
  host: "localhost"
  port: 8080

fault:
  http:
    enabled: true
    failure_probability: 0.05        # 5%的HTTP请求失败概率
    rate_limit_probability: 0.02     # 2%的429限流错误概率
    server_error_probability: 0.03   # 3%的5xx服务器错误概率
    
  websocket:
    enabled: true
    disconnection_probability: 0.01        # 1%的连接断开概率
    data_loss_probability: 0.02           # 2%的数据丢失概率
    heartbeat_anomaly_probability: 0.01   # 1%的心跳异常概率
  
  chain_reorg:
    enabled: false                    # 是否启用链重组模拟
    probability: 0.001                # 链重组发生概率(0.1%)
    reorg_depth_min: 1                # 最小回退区块数
    reorg_depth_max: 5                # 最大回退区块数

data:
  ethereum:
    block_interval: 12          # 区块间隔（秒）
    start_block_number: 1000000 # 起始区块号
    persistence:
      enabled: true                          # 是否启用持久化
      state_file: "./data/block_state.json"  # 状态文件路径
      save_interval: 10                      # 保存间隔（秒）
    crash_simulation:
      enabled: false                         # 是否启用宕机模拟
      lost_blocks: 5                         # 宕机时丢失的区块数量
```

### 持久化配置说明

- **persistence.enabled**: 启用后，服务会定期保存当前区块状态到文件，重启后自动恢复
- **persistence.state_file**: 状态文件保存路径，默认为`./data/block_state.json`
- **persistence.save_interval**: 定期保存的间隔时间（秒），建议设置为10-60秒

### 宕机模拟配置说明

- **crash_simulation.enabled**: 启用后，服务重启时会跳过指定数量的区块，模拟真实宕机场景
- **crash_simulation.lost_blocks**: 宕机期间"丢失"的区块数量，例如设置为5表示重启后区块号会在上次的基础上+5

### 使用场景示例

#### 场景1: 正常持久化（不丢失区块）
```yaml
persistence:
  enabled: true
  state_file: "./data/block_state.json"
  save_interval: 10
crash_simulation:
  enabled: false
  lost_blocks: 0
```
服务重启后会从上次的区块号继续，不会丢失任何区块。

#### 场景2: 宕机模拟（丢失部分区块）
```yaml
persistence:
  enabled: true
  state_file: "./data/block_state.json"
  save_interval: 10
crash_simulation:
  enabled: true
  lost_blocks: 10
```
服务重启后会在上次区块号基础上+10，模拟宕机期间错过了10个区块的场景。

### 链重组配置说明

- **chain_reorg.enabled**: 启用后，服务会随机触发链重组，模拟区块链分叉和重组场景
- **chain_reorg.probability**: 每次生成区块时触发重组的概率，建议设置为较低值(如0.001表示千分之一)
- **chain_reorg.reorg_depth_min**: 重组时最少回退的区块数
- **chain_reorg.reorg_depth_max**: 重组时最多回退的区块数

### 链重组使用场景

链重组是区块链中的常见现象，当两个矿工几乎同时挖出区块时，会产生临时分叉。当一条链变得更长时，节点会切换到更长的链，导致短链上的区块被"回退"。

#### 场景: 测试系统对链重组的容错能力
```yaml
chain_reorg:
  enabled: true
  probability: 0.01    # 1%的概率触发重组
  reorg_depth_min: 1   # 最少回退1个区块
  reorg_depth_max: 3   # 最多回退3个区块
```

启用后，系统会：
1. 在生成新区块前随机触发重组
2. 回退1-3个区块(随机)
3. 从新的分叉点继续生成区块
4. 模拟真实链重组场景，测试数据接入系统的处理能力

**注意**: 链重组发生时，区块号会回退，但区块hash会改变(模拟新分叉)，这样可以测试系统是否能正确处理已处理过的区块被重组的情况。

## 架构设计

### 目录结构
```
mock-service/
├── cmd/server/          # 服务器启动入口
├── internal/
│   ├── config/         # 配置管理
│   ├── controller/     # HTTP和WebSocket控制器
│   ├── fault/          # 故障注入器
│   ├── generator/      # 数据生成器
│   └── model/          # 数据模型
├── configs/            # 配置文件
├── go.mod
├── go.sum
├── main.go
└── README.md
```

### 核心组件

1. **DataGenerator**: 负责生成模拟的以太坊区块头数据
2. **FaultInjector**: 实现各种故障注入逻辑
3. **WebSocketController**: 处理WebSocket连接和订阅
4. **HTTPController**: 处理HTTP JSON-RPC请求

## 监控和调试

### 查看故障注入统计
```bash
curl http://localhost:8080/fault/stats
```

### 重置故障注入统计
```bash
curl -X POST http://localhost:8080/fault/reset
```

### 健康检查
```bash
curl http://localhost:8080/health
```

## 故障注入测试

服务会根据配置的概率自动注入各种故障，用于测试客户端的容错能力：

- **HTTP 429错误**: 模拟API限流
- **HTTP 5xx错误**: 模拟服务器内部错误
- **WebSocket断开**: 模拟网络连接问题
- **数据丢失**: 模拟部分数据未成功传输
- **心跳异常**: 模拟心跳检测失败
- **链重组**: 模拟区块链分叉重组，回退若干区块后从新分叉继续

## 扩展开发

如需添加新的数据源类型或故障类型，可以：

1. 在`internal/model/`中添加新的数据模型
2. 在`internal/generator/`中实现新的数据生成逻辑
3. 在`internal/fault/`中添加新的故障注入类型
4. 在`internal/controller/`中实现新的控制器逻辑

## 注意事项

- 这是一个用于测试的mock服务，不应在生产环境中使用
- 故障注入是随机的，实际测试时可能需要多次运行才能触发所有故障类型
- 服务支持优雅关闭，可以通过Ctrl+C或发送SIGTERM信号来停止
- 所有日志都会输出到控制台，便于调试和监控

## 许可证

本项目仅供学习和测试使用。