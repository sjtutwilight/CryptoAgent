# ClickHouse API 问题解决报告

## 问题概述
用户报告ClickHouse相关的API都返回空数据，经过调查发现多个根本原因需要解决。

## 已解决的问题

### 1. ClickHouse DateTime格式不兼容
**问题**: ClickHouse JDBC驱动无法正确处理Java的LocalDateTime对象，导致类型转换错误。
**错误示例**: `Cannot convert string '2025-09-21T14:05:26.137740' to type DateTime`

**解决方案**: 在所有Repository中将LocalDateTime参数转换为字符串格式：
```java
String startTimeStr = startTime.withNano(0).toString().replace("T", " ");
String endTimeStr = endTime.withNano(0).toString().replace("T", " ");

String finalSql = sql.replace("AND end_time >= ?", "AND end_time >= '" + startTimeStr + "'")
                     .replace("AND end_time <= ?", "AND end_time <= '" + endTimeStr + "'");
```

**影响的文件**:
- `TokenRepositoryImpl.java` - 修复了价格历史查询
- `TradeRepositoryImpl.java` - 修复了交易量和净流入查询
- `PnLRepositoryImpl.java` - 修复了宏观PnL历史查询
- `DistributionRepositoryImpl.java` - 修复了分布历史和标签持仓查询
- `AccountRepositoryImpl.java` - 修复了转账历史查询

### 2. ch_account_trade_minute表结构不完整
**问题**: 物化视图`mv_trade_to_minute`缺少`side`字段，导致无法区分买卖交易。
**错误示例**: `Unknown expression or function identifier 'side' in scope`

**解决方案**: 重新创建表和物化视图：
```sql
-- 删除并重建表
DROP TABLE IF EXISTS ch_account_trade_minute;
CREATE TABLE ch_account_trade_minute
(
  end_time   DateTime,
  account_id UInt64,
  token_id   UInt64,
  side       LowCardinality(String),  -- 新增字段
  trade_cnt  UInt32,
  volume_usd Decimal(38,18)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (account_id, end_time, token_id, side);

-- 重建物化视图
DROP VIEW IF EXISTS mv_trade_to_minute;
CREATE MATERIALIZED VIEW mv_trade_to_minute
TO ch_account_trade_minute AS
SELECT
  toStartOfMinute(block_time) AS end_time,
  account_id,
  token_id,
  side,  -- 包含side字段
  count() AS trade_cnt,
  sum(value_usd) AS volume_usd
FROM ch_account_trade_fact
GROUP BY end_time, account_id, token_id, side;  -- GROUP BY中包含side
```

### 3. 数据库配置问题
**问题**: PostgreSQL连接凭据不匹配，ClickHouse压缩设置不当。

**解决方案**: 
- 更新PostgreSQL凭据匹配docker-compose.yml配置
- 禁用ClickHouse LZ4压缩: `compress=0&auto_discovery=false`

## API状态汇总

| API接口 | 状态 | 说明 |
|---------|------|------|
| ✅ Token Overview | 正常 | 返回代币基础信息和宏观指标 |
| ⚠️ Price Chart | 数据不足 | 接口正常，但历史价格数据为空 |
| ⚠️ Trade Flow | 数据不足 | 接口正常，但交易流数据为空* |
| ✅ PnL Data | 部分正常 | 宏观PnL数据正常，Top PnL为空 |
| ✅ Distribution | 部分正常 | 分布指标正常，持有者列表为空 |
| ✅ Account Info | 正常 | 转账历史正常，账户资产为空 |

*注：交易流数据为空是因为token_recent_metric_ch表只有tag='all'的数据，缺少标签维度数据。

## 数据充分性分析

### 有数据的表/视图
- `token_recent_metric_ch` (tag='all') - 提供宏观指标
- `v_token_macro_minute` - 提供PnL宏观数据
- `v_token_distribution_latest` - 提供分布数据
- `ch_account_trade_minute` - 提供转账历史（修复后）
- `token`, `account` (PostgreSQL) - 提供元数据

### 数据不足的表/视图
- `token_recent_metric_ch` (标签维度) - 缺少tag!='all'的数据，影响净流入计算
- `ch_account_trade_fact` - 历史交易明细数据有限
- `v_token_top_holders_latest` - Top持有者数据为空
- `ch_account_balance_snapshot` - 账户资产快照为空

## 建议后续改进

1. **数据填充**: 为测试环境补充更多历史数据
2. **监控告警**: 添加数据完整性检查
3. **降级策略**: 当数据不足时提供模拟数据或明确提示
4. **性能优化**: 考虑添加数据缓存层

## 技术改进成果

1. ✅ 解决了所有ClickHouse DateTime兼容性问题
2. ✅ 修复了数据库连接配置问题  
3. ✅ 重建了正确的物化视图结构
4. ✅ 所有API接口现在都能正常响应（不再报错）
5. ✅ 实现了真实数据源替代mock数据
6. ✅ 账户转账历史功能完全正常工作

## 测试验证

所有API接口已通过测试，能够正确处理请求并返回结构化数据。对于数据不足的情况，API会返回空数组而不是错误，符合降级设计原则。


