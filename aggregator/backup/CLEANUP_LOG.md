# Aggregator工程清理日志

## 清理时间
2025年1月

## 清理原因
在ProcessEvent强类型重构和标准化算子流程后，许多旧的Processor和组件已被新的实现替代，为保持代码库整洁，将废弃文件移动到backup目录。

## 废弃文件列表

### deprecated-processors/ - 废弃的处理器
1. **EventSplitProcessor.java**
   - 替代方案：各Job内部直接使用对应的Extractor
   - 废弃原因：不再需要通过侧流分发，统一在不同job中处理

2. **UnifiedEventProcessor.java**
   - 替代方案：UnifiedFilterOperator
   - 废弃原因：功能被新的过滤器和增强器拆分替代

3. **MetadataEnrichmentOperator.java**
   - 替代方案：AsyncEventEnrichmentProcessor
   - 废弃原因：改为异步处理元数据，不再使用广播状态

4. **PriceBroadcastProcessor.java**
   - 替代方案：RedisTokenMetricsBroadcaster
   - 废弃原因：价格广播逻辑已整合到新的广播器中

5. **BalanceDeltaPriceBroadcaster.java**
   - 替代方案：新的标准化价格广播流
   - 废弃原因：已被统一的价格广播机制替代

## 新的标准化架构

### 算子流程
```
KafkaMessage 
→ UnifiedFilterOperator (事件过滤和强类型转换)
→ AsyncEventEnrichmentProcessor (异步元数据增强)
→ RedisTokenMetricsBroadcaster (价格广播)
→ Job特定Extractors (业务逻辑提取)
→ 特殊算子 (DualStreamAligner, PnLProcessor, TokenSlidingWindowProcessor, PairWindowProcessor)
```

### 强类型数据模型
- ERC20TransferData - ERC20转账事件
- DexSwapData - DEX交换事件
- LPMintData - LP铸造事件
- LPBurnData - LP销毁事件

## 注意事项
- backup中的文件仅供参考，不应在生产环境中使用
- 如需回滚或参考旧实现，可以从backup目录中找到
- 定期清理backup目录中不再需要的文件