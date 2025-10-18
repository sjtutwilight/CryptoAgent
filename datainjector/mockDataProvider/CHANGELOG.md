# 更新日志

## [未发布] - 2025-10-18

### 新增功能

#### 1. 区块持久化功能
- 支持定期保存区块状态到文件
- 服务重启后自动恢复到上次的区块号
- 配置项:
  - `data.ethereum.persistence.enabled`: 启用持久化
  - `data.ethereum.persistence.state_file`: 状态文件路径
  - `data.ethereum.persistence.save_interval`: 保存间隔(秒)

#### 2. 宕机模拟功能
- 支持模拟服务宕机期间丢失若干区块
- 重启后区块号会跳过配置的数量
- 配置项:
  - `data.ethereum.crash_simulation.enabled`: 启用宕机模拟
  - `data.ethereum.crash_simulation.lost_blocks`: 丢失的区块数

#### 3. 链重组故障注入
- 支持模拟区块链重组(Chain Reorg)场景
- 可配置回退区块数范围和触发概率
- 模拟真实的分叉和链切换行为
- 配置项:
  - `fault.chain_reorg.enabled`: 启用链重组模拟
  - `fault.chain_reorg.probability`: 触发概率
  - `fault.chain_reorg.reorg_depth_min`: 最小回退区块数
  - `fault.chain_reorg.reorg_depth_max`: 最大回退区块数

### Bug修复

#### WebSocket并发写入问题
- 修复了多个goroutine同时写入WebSocket导致panic的问题
- 为每个连接添加了写锁(`writeMu`)保护
- 影响的方法:
  - `sendResponse`: 发送JSON-RPC响应
  - `generateAndBroadcastBlock`: 广播新区块
  - ping/pong心跳处理

### 改进

#### 日志优化
- 持久化操作添加详细日志标记 `[持久化]`
- 链重组事件添加日志标记 `[链重组]` 和 `[链重组故障]`
- 宕机模拟添加日志标记 `[宕机模拟]`
- 便于观察和调试各种故障场景

#### 优雅关闭
- DataGenerator支持优雅关闭
- 关闭时自动保存最后状态
- 避免数据丢失

### 文档更新

- 新增 `docs/CHAIN_REORG_GUIDE.md`: 链重组功能详细指南
- 更新 `README.md`: 添加持久化、宕机模拟、链重组说明
- 新增 `.gitignore`: 排除数据文件和临时文件

### 配置文件变更

新增配置项(向后兼容，默认值已设置):
```yaml
# 持久化配置
data.ethereum.persistence.enabled: true
data.ethereum.persistence.state_file: "./data/block_state.json"
data.ethereum.persistence.save_interval: 10

# 宕机模拟配置
data.ethereum.crash_simulation.enabled: false
data.ethereum.crash_simulation.lost_blocks: 0

# 链重组配置
fault.chain_reorg.enabled: false
fault.chain_reorg.probability: 0.001
fault.chain_reorg.reorg_depth_min: 1
fault.chain_reorg.reorg_depth_max: 5
```

### 技术细节

#### 并发安全
- 为WebSocket连接添加写互斥锁，防止并发写入panic
- DataGenerator的读写操作已有RWMutex保护
- FaultInjector的统计信息有RWMutex保护

#### 持久化实现
- 使用原子文件写入(临时文件+重命名)
- JSON格式存储，包含区块号、hash和时间戳
- 后台定时保存，不影响主流程性能

#### 链重组实现
- 在生成新区块前检查是否触发重组
- 回退区块号并重新生成hash模拟新分叉
- 统计信息记录重组次数

### 注意事项

1. **WebSocket并发**: 升级后必须使用新版本，旧版本存在并发写入bug
2. **持久化文件**: 默认保存在 `./data/` 目录，需确保有写权限
3. **链重组概率**: 建议设置较低值(0.001-0.01)，避免过于频繁
4. **向后兼容**: 所有新配置项都有默认值，不修改配置文件也能正常运行

### 测试建议

1. 测试持久化: 启动服务，观察区块生成，停止后重启，验证区块号连续
2. 测试宕机模拟: 启用crash_simulation，重启后验证区块号跳过
3. 测试链重组: 启用chain_reorg，观察日志中的重组事件
4. 测试并发: 多客户端同时订阅WebSocket，验证无panic
5. 组合测试: 同时启用多种故障，验证系统稳定性

