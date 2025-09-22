# WebSocket Worker

WebSocket Worker 模块用于对接外部 WebSocket API 完成数据拉取，并将数据写入 Kafka 数据通道。该模块支持断线重连、心跳机制，并集成了 Binance 和 QuickNode 数据源。

## 功能特性

- **连接管理**: 自动断线重连、心跳检测 (ping/pong)
- **多数据源支持**: Binance WebSocket Stream、QuickNode Ethereum API
- **Kafka 集成**: 使用 common 模块的统一 Kafka 组件
- **配置管理**: YAML 配置文件支持
- **日志记录**: 结构化日志记录
- **优雅关闭**: 支持信号处理和优雅关闭

## 项目结构

```
websocket-worker/
├── main.go                     # 主入口文件
├── go.mod                      # Go 模块依赖
├── configs/
│   └── config.yaml            # 配置文件
├── cmd/
│   └── main.go                # 命令行入口
├── internal/
│   ├── config/
│   │   └── config.go          # 配置管理
│   ├── connection/
│   │   └── manager.go         # 连接管理器
│   ├── client/
│   │   ├── binance_client.go  # Binance WebSocket 客户端
│   │   └── quicknode_client.go # QuickNode WebSocket 客户端
│   └── producer/
│       └── producer.go        # Kafka 数据生产者
└── README.md                  # 项目文档
```

## 配置说明

### Kafka 配置
```yaml
kafka:
  brokers:
    - "localhost:9092"
  topics:
    binance: "binance_data"
    quicknode: "quicknode_data"
```

### WebSocket 配置
```yaml
websocket:
  binance:
    url: "wss://stream.binance.com:9443/ws/stream"
    apiKey: "your-api-key"
    secretKey: "your-secret-key"
    symbols:
      - "btcusdt"
      - "ethusdt"
      - "adausdt"
      - "bnbusdt"
      - "xrpusdt"
    interval: "1m"
    
  quicknode:
    url: "wss://example.quiknode.pro/your-endpoint/"
    apiKey: "your-api-key"
    subscriptions:
      - "newHeads"
```

### 连接配置
```yaml
connection:
  heartbeat:
    interval: 30  # seconds
    timeout: 10   # seconds
  reconnect:
    maxRetries: 5
    backoffBase: 2  # seconds
    backoffMax: 60  # seconds
```

## 数据源

### Binance WebSocket Stream
- **接入内容**: K线数据 (`<symbol>@kline_<interval>`)
- **支持交易对**: BTC/USDT, ETH/USDT, ADA/USDT, BNB/USDT, XRP/USDT
- **数据格式**: JSON 格式的 K线数据
- **Topic**: `binance_data`

### QuickNode Ethereum API
- **接入内容**: 以太坊区块链原生数据
- **订阅类型**: `newHeads` (新区块头)
- **数据格式**: JSON 格式的区块数据
- **Topic**: `quicknode_data`

## 构建和运行

### 本地开发
```bash
# 安装依赖
go mod tidy

# 运行程序
go run main.go -config configs/config.yaml -log-level debug

# 或者使用 cmd 目录
go run cmd/main.go -config configs/config.yaml
```

### 构建二进制文件
```bash
# 构建
go build -o websocket-worker main.go

# 运行
./websocket-worker -config configs/config.yaml
```

### Docker 部署
```bash
# 构建镜像
docker build -t websocket-worker .

# 运行容器
docker run -d \
  --name websocket-worker \
  -v $(pwd)/configs:/app/configs \
  websocket-worker
```

## 使用方法

### 启动服务
```bash
./websocket-worker -config configs/config.yaml -log-level info
```

### 命令行参数
- `-config`: 配置文件路径 (默认: configs/config.yaml)
- `-log-level`: 日志级别 (debug/info/warn/error, 默认: info)

### 监控
- 查看日志输出了解连接状态和数据流
- 监控 Kafka topic 中的数据
- 检查 WebSocket 连接状态和重连情况

## 依赖组件

### Common 模块
- **Kafka Producer**: 统一的 Kafka 生产者接口
- **Kafka Consumer**: 统一的 Kafka 消费者接口

### 外部依赖
- **Gorilla WebSocket**: WebSocket 客户端库
- **Sarama**: Kafka Go 客户端
- **Logrus**: 结构化日志库
- **YAML**: 配置文件解析

## 故障处理

### 连接异常
- 自动重连机制 (指数退避)
- 最大重试次数限制
- 连接状态监控

### 数据异常
- 消息格式验证
- 错误日志记录
- 继续处理其他消息

### Kafka 异常
- 发送超时处理
- 错误重试机制
- 生产者连接管理

## 性能优化

- 使用连接池管理 WebSocket 连接
- 批量发送 Kafka 消息
- 异步消息处理
- 内存使用优化

## 监控指标

- WebSocket 连接状态
- 消息接收/发送速率
- Kafka 生产者指标
- 错误率和延迟

## 开发指南

### 添加新数据源
1. 在 `internal/client/` 目录下创建新的客户端
2. 实现连接管理和消息处理
3. 在配置文件中添加相应配置
4. 在主应用程序中集成新客户端

### 扩展消息格式
1. 在 `internal/producer/` 中定义新的数据结构
2. 更新生产者接口和实现
3. 添加相应的序列化逻辑

## 注意事项

- 确保 Kafka 集群可访问
- 检查 API 密钥的有效性
- 监控网络连接稳定性
- 定期更新依赖库版本

