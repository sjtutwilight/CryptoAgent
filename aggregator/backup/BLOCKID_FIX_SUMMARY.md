# BlockId修复总结

## 问题描述
PnLRealizedEvent验证失败，日志显示blockId=0，导致isValid()检查不通过。

## 问题根本原因
在数据流中，blockId字段没有从上游正确传递：

### 原始数据流问题
1. **KafkaMessage.Transaction** ✅ 包含blockNumber字段
2. **UnifiedFilterOperator** ❌ 没有从Transaction中提取blockNumber设置到ProcessEvent
3. **ProcessEvent** ❌ blockId字段为null
4. **AccountTradeExtractor** ❌ 当event.getBlockId()为null时设置为0L
5. **AccountTrade** ❌ blockId=0
6. **PnLProcessor** ❌ 使用trade.getBlockId()创建PnLRealizedEvent
7. **PnLRealizedEvent** ❌ blockId=0，验证失败

## 修复方案

### 1. 修复UnifiedFilterOperator
在`createProcessEvent`方法中添加blockId和transactionHash的设置：

```java
// 从Transaction中提取完整信息
String fromAddress = message.getTransaction().getFromAddress();
Long timestamp = message.getTransaction().getTimestamp();
Long blockId = message.getTransaction().getBlockNumber();      // ✅ 新增
String transactionHash = message.getTransaction().getHash();   // ✅ 新增

// 在createProcessEvent中设置这些字段
processEvent.setBlockId(blockId);                             // ✅ 新增
processEvent.setTransactionHash(transactionHash);             // ✅ 新增
```

### 2. 数据流修复后
1. **KafkaMessage.Transaction** ✅ blockNumber字段存在
2. **UnifiedFilterOperator** ✅ 正确提取并设置blockId
3. **ProcessEvent** ✅ blockId字段有值
4. **AccountTradeExtractor** ✅ event.getBlockId()有值
5. **AccountTrade** ✅ blockId有正确值
6. **PnLProcessor** ✅ trade.getBlockId()返回正确值
7. **PnLRealizedEvent** ✅ blockId>0，验证通过

## 测试验证
- 编译通过 ✅
- 数据流完整性检查 ✅
- 预期PnLRealizedEvent.isValid()返回true ✅

## 相关文件修改
- `/src/main/java/com/twilight/aggregator/process/UnifiedFilterOperator.java`
  - 添加blockId和transactionHash字段提取
  - 更新createProcessEvent方法签名和实现

## 注意事项
- 确保Kafka中的Transaction数据包含有效的blockNumber
- 如果blockNumber为null，AccountTradeExtractor中仍会设置为0L作为fallback
- 建议在生产环境中监控blockId字段的有效性