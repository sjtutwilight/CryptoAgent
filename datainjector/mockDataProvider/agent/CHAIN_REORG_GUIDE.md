# 链重组故障注入功能使用指南

## 功能概述

链重组(Chain Reorg)是区块链中的常见现象，当网络中出现临时分叉时，节点会切换到最长链，导致短链上的区块被回退。本功能模拟这一场景，用于测试数据接入系统对链重组的容错能力。

## 配置说明

在 `configs/config.yaml` 中配置链重组参数:

```yaml
fault:
  chain_reorg:
    enabled: true                    # 是否启用链重组模拟
    probability: 0.001               # 链重组发生概率(千分之一)
    reorg_depth_min: 1               # 最小回退区块数
    reorg_depth_max: 5               # 最大回退区块数
```

### 参数说明

- **enabled**: 总开关，设置为 `true` 启用链重组模拟
- **probability**: 每次生成新区块时触发重组的概率
  - 0.001 = 0.1% = 千分之一
  - 0.01 = 1% = 百分之一
  - 建议设置较低值，避免频繁触发
- **reorg_depth_min**: 重组时最少回退的区块数量
- **reorg_depth_max**: 重组时最多回退的区块数量
  - 实际回退数量会在 [min, max] 范围内随机选择

## 工作原理

### 1. 触发时机
每次生成新区块前，系统会按照配置的概率随机决定是否触发链重组。

### 2. 重组过程
当触发链重组时:
1. 系统会回退指定数量的区块(在配置范围内随机)
2. 重新生成 `lastBlockHash`，模拟切换到新的分叉
3. 从回退后的区块号继续生成新区块
4. 新生成的区块会有不同的hash值

### 3. 日志输出
```
[链重组故障] 触发链重组,回退 3 个区块
[链重组] 发生链重组: 回退 3 个区块, 1000125 -> 1000122
```

## 使用场景

### 场景1: 测试轻度重组(1-2个区块)
```yaml
chain_reorg:
  enabled: true
  probability: 0.01        # 1%触发概率
  reorg_depth_min: 1
  reorg_depth_max: 2
```
**用途**: 测试系统对最常见的轻微分叉的处理能力

### 场景2: 测试中等重组(3-5个区块)
```yaml
chain_reorg:
  enabled: true
  probability: 0.005       # 0.5%触发概率
  reorg_depth_min: 3
  reorg_depth_max: 5
```
**用途**: 测试系统对较严重分叉的处理能力

### 场景3: 测试深度重组(5-10个区块)
```yaml
chain_reorg:
  enabled: true
  probability: 0.001       # 0.1%触发概率
  reorg_depth_min: 5
  reorg_depth_max: 10
```
**用途**: 测试系统对罕见的深度分叉的处理能力

### 场景4: 频繁重组压力测试
```yaml
chain_reorg:
  enabled: true
  probability: 0.05        # 5%触发概率
  reorg_depth_min: 1
  reorg_depth_max: 3
```
**用途**: 压力测试，验证系统在频繁重组场景下的稳定性

## 验证方法

### 1. 观察日志
启动服务后，观察日志输出:
```bash
go run main.go
```

当触发链重组时，会看到类似输出:
```
2025/10/18 14:30:45 生成新区块: 0xabc..., 高度: 0xf42c8
2025/10/18 14:30:47 [链重组故障] 触发链重组,回退 2 个区块
2025/10/18 14:30:47 [链重组] 发生链重组: 回退 2 个区块, 1000200 -> 1000198
2025/10/18 14:30:47 生成新区块: 0xdef..., 高度: 0xf42c7
```

### 2. 查看统计信息
```bash
curl http://localhost:8090/fault/stats
```

响应示例:
```json
{
  "http_failure": 5,
  "http_rate_limit": 3,
  "http_server_error": 2,
  "websocket_disconnection": 1,
  "websocket_data_loss": 4,
  "websocket_heartbeat_anomaly": 2,
  "chain_reorg": 3
}
```

`chain_reorg` 字段显示链重组发生的次数。

### 3. 客户端验证
在数据接入系统(Worker)中，应该能观察到:
- 接收到的区块号出现回退
- 相同区块号的区块hash不同(旧分叉vs新分叉)
- 系统正确处理重组，重新处理受影响的区块

## 与其他功能的配合

### 与持久化功能配合
```yaml
data:
  ethereum:
    persistence:
      enabled: true
      save_interval: 10

fault:
  chain_reorg:
    enabled: true
    probability: 0.01
    reorg_depth_min: 1
    reorg_depth_max: 3
```
链重组发生后，新的区块状态会被持久化，重启后从重组后的位置继续。

### 与宕机模拟配合
```yaml
data:
  ethereum:
    crash_simulation:
      enabled: true
      lost_blocks: 10

fault:
  chain_reorg:
    enabled: true
    probability: 0.01
    reorg_depth_min: 1
    reorg_depth_max: 5
```
可以同时测试宕机恢复和链重组处理。

## 注意事项

1. **概率设置**: 建议根据测试需求合理设置概率，避免过于频繁影响正常测试
2. **回退深度**: 实际以太坊网络中，深度超过6个区块的重组极为罕见
3. **区块号限制**: 回退不会超过起始区块号(`start_block_number`)
4. **分叉模拟**: 重组后生成的区块hash会改变，模拟切换到新分叉
5. **统计重置**: 可通过 `/fault/reset` 接口重置统计数据

## 常见问题

### Q1: 为什么设置了链重组但没有触发?
A: 链重组是概率性触发的。如果概率设置很低(如0.001)，可能需要生成1000个区块才会触发一次。可以提高概率或观察更长时间。

### Q2: 链重组会影响持久化的状态吗?
A: 会。链重组后的新状态会在下次定期保存时被持久化。重启后会从重组后的状态恢复。

### Q3: 如何验证客户端正确处理了链重组?
A: 观察客户端日志，应该能看到:
- 检测到区块号回退
- 检测到相同区块号但hash不同
- 触发重新处理逻辑
- 数据最终一致性保持正确

### Q4: 可以手动触发链重组吗?
A: 当前版本是自动触发。如需手动触发，可以临时设置很高的概率(如0.9)，生成几个区块后触发，然后改回正常值。

## 扩展建议

如需更复杂的链重组场景，可以考虑扩展:
1. 支持指定固定回退深度
2. 支持手动触发API
3. 支持配置重组后的新分叉行为
4. 记录详细的重组事件日志

