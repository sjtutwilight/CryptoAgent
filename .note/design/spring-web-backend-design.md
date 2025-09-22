# Spring Boot 2.x Web后端设计文档

## 1. 项目架构概述

### 1.1 技术栈
- **框架**: Spring Boot 2.x
- **数据库**: 
  - ClickHouse (主要数据查询)
  - PostgreSQL (元数据存储)
- **连接池**: HikariCP
- **序列化**: Jackson
- **文档**: SpringDoc OpenAPI 3
- **监控**: Spring Boot Actuator

### 1.2 模块结构
```
backend/
├── src/main/java/com/twilight/backend/
│   ├── config/          # 配置类
│   ├── controller/      # REST控制器
│   ├── service/         # 业务服务层
│   ├── repository/      # 数据访问层
│   ├── model/          # 数据模型
│   ├── dto/            # 数据传输对象
│   └── util/           # 工具类
├── src/main/resources/
│   ├── application.yml
│   └── sql/            # SQL查询文件
└── pom.xml
```

## 2. 数据模型设计

### 2.1 代币相关模型

```java
// 代币基础信息
@Data
public class TokenInfo {
    private Long tokenId;
    private String chainName;
    private String symbol;
    private String name;           // 使用symbol
    private Integer age;           // 随机值
    private String tokenCategory;
    private Integer securityScore; // 随机值
    private String issuer;
    private String address;
    private Integer decimals;
}

// 代币宏观指标
@Data
public class TokenMetrics {
    private Long tokenId;
    private LocalDateTime endTime;
    private BigDecimal currentPrice;
    private BigDecimal fdv;
    private BigDecimal mcap;
    private BigDecimal liquidity;
    private BigDecimal fdvMcapRatio;
    private BigDecimal mcapLiquidityRatio;
    private BigDecimal fdvLiquidityRatio;
}

// 代币历史价格
@Data
public class TokenPriceHistory {
    private Long tokenId;
    private LocalDateTime endTime;
    private BigDecimal price;
    private BigDecimal priceChange;
    private BigDecimal priceChangePercent;
}
```

### 2.2 交易流相关模型

```java
// DEX交易量统计
@Data
public class TokenTradeVolume {
    private Long tokenId;
    private String timeWindow;
    private LocalDateTime endTime;
    private String tag;
    private Integer txCount;
    private Integer buyCount;
    private Integer sellCount;
    private BigDecimal volumeUsd;
    private BigDecimal buyVolumeUsd;
    private BigDecimal sellVolumeUsd;
    private BigDecimal buyPressureUsd;
}

// 标签净流入数据
@Data
public class TagNetFlow {
    private Long tokenId;
    private LocalDateTime endTime;
    private String tag;
    private BigDecimal netFlowUsd;
    private BigDecimal inflowUsd;
    private BigDecimal outflowUsd;
    private Integer tradersCount;
}

// DEX交易明细
@Data
public class DexTradeDetail {
    private String txHash;
    private LocalDateTime blockTime;
    private String fromAddress;
    private String toAddress;
    private Long tokenId;
    private String tokenSymbol;
    private String tokenAddress;
    private String side;
    private BigDecimal qty;
    private BigDecimal priceUsd;
    private BigDecimal valueUsd;
    private List<String> labels;
}
```

### 2.3 PnL相关模型

```java
// Top PnL信息
@Data
public class TopPnLInfo {
    private Long accountId;
    private String address;
    private Long tokenId;
    private BigDecimal totalPnlUsd;
    private Double roiPercent;
    private BigDecimal realizedPnlUsd;
    private BigDecimal unrealizedPnlUsd;
    private Double stillHoldingPercent;
    private List<String> labels;
}

// 宏观PnL指标
@Data
public class MacroPnLMetrics {
    private Long tokenId;
    private LocalDateTime endTime;
    private BigDecimal mcapUsd;
    private BigDecimal realizedCapUsd;
    private BigDecimal networkValueUsd;
    private Double nupl;
    private Double mvrv;
    private Double sopr;
    private BigDecimal realizedPnlUsd;
}
```

### 2.4 代币分布相关模型

```java
// 代币分布宏观指标
@Data
public class TokenDistribution {
    private Long tokenId;
    private LocalDateTime endTime;
    private Integer holdersCount;
    private Double top2SharePercent;
    private BigDecimal medianHolderValueUsd;
    private Double freshHolderCountShare;
    private Double concentrationIndex;
}

// 标签维度持仓
@Data
public class TagHolding {
    private Long tokenId;
    private LocalDateTime endTime;
    private String tag;
    private BigDecimal valueUsd;
    private Integer holdersCount;
    private Double changePercent1Min;
}

// Top Holder明细
@Data
public class TopHolder {
    private Long accountId;
    private String address;
    private List<String> labels;
    private Double ownershipPercent;
    private BigDecimal balance;
    private BigDecimal valueUsd;
}
```

### 2.5 账户相关模型

```java
// 账户基础信息
@Data
public class AccountInfo {
    private Long accountId;
    private String chainName;
    private String address;
    private String entity;
    private List<String> labels;
}

// 账户资产
@Data
public class AccountAsset {
    private String assetType;  // native/erc20/defiPosition
    private Long bizId;
    private String bizName;
    private BigDecimal amount;
    private BigDecimal valueUsd;
    private BigDecimal price;
}

// 账户转账历史聚合
@Data
public class AccountTransferHistory {
    private Long accountId;
    private LocalDateTime endTime;
    private Integer buyTxCount;
    private Integer sellTxCount;
    private BigDecimal buyVolumeUsd;
    private BigDecimal sellVolumeUsd;
}
```

## 3. API接口设计

### 3.1 代币大盘接口

```java
@RestController
@RequestMapping("/api/v1/tokens")
public class TokenController {
    
    // 获取代币基础信息
    @GetMapping("/{tokenId}/info")
    public TokenInfo getTokenInfo(@PathVariable Long tokenId);
    
    // 获取代币宏观指标
    @GetMapping("/{tokenId}/metrics")
    public TokenMetrics getTokenMetrics(@PathVariable Long tokenId);
    
    // 获取代币历史价格
    @GetMapping("/{tokenId}/price-history")
    public List<TokenPriceHistory> getPriceHistory(
        @PathVariable Long tokenId,
        @RequestParam String timeRange
    );
    
    // 获取代币交易量统计
    @GetMapping("/{tokenId}/trade-volume")
    public List<TokenTradeVolume> getTradeVolume(
        @PathVariable Long tokenId,
        @RequestParam String timeWindow,
        @RequestParam(required = false) String tag
    );
    
    // 获取标签净流入数据
    @GetMapping("/{tokenId}/net-flow")
    public List<TagNetFlow> getNetFlow(
        @PathVariable Long tokenId,
        @RequestParam String timeRange
    );
    
    // 获取DEX交易明细
    @GetMapping("/{tokenId}/trades")
    public PageResult<DexTradeDetail> getDexTrades(
        @PathVariable Long tokenId,
        @RequestParam(defaultValue = "1") Integer page,
        @RequestParam(defaultValue = "20") Integer size,
        @RequestParam(required = false) String timeRange
    );
}
```

### 3.2 PnL接口

```java
@RestController
@RequestMapping("/api/v1/pnl")
public class PnLController {
    
    // 获取Top PnL信息
    @GetMapping("/tokens/{tokenId}/top")
    public List<TopPnLInfo> getTopPnL(
        @PathVariable Long tokenId,
        @RequestParam(defaultValue = "50") Integer limit
    );
    
    // 获取宏观PnL指标
    @GetMapping("/tokens/{tokenId}/macro")
    public MacroPnLMetrics getMacroPnL(@PathVariable Long tokenId);
    
    // 获取宏观指标历史
    @GetMapping("/tokens/{tokenId}/macro/history")
    public List<MacroPnLMetrics> getMacroPnLHistory(
        @PathVariable Long tokenId,
        @RequestParam String timeRange
    );
}
```

### 3.3 代币分布接口

```java
@RestController
@RequestMapping("/api/v1/distribution")
public class DistributionController {
    
    // 获取代币分布宏观指标
    @GetMapping("/tokens/{tokenId}")
    public TokenDistribution getTokenDistribution(@PathVariable Long tokenId);
    
    // 获取标签维度持仓
    @GetMapping("/tokens/{tokenId}/tags")
    public List<TagHolding> getTagHoldings(@PathVariable Long tokenId);
    
    // 获取标签持仓历史
    @GetMapping("/tokens/{tokenId}/tags/history")
    public List<TagHolding> getTagHoldingsHistory(
        @PathVariable Long tokenId,
        @RequestParam String timeRange
    );
    
    // 获取Top Holder明细
    @GetMapping("/tokens/{tokenId}/top-holders")
    public List<TopHolder> getTopHolders(
        @PathVariable Long tokenId,
        @RequestParam(defaultValue = "100") Integer limit
    );
}
```

### 3.4 账户接口

```java
@RestController
@RequestMapping("/api/v1/accounts")
public class AccountController {
    
    // 获取账户基础信息
    @GetMapping("/{accountId}/info")
    public AccountInfo getAccountInfo(@PathVariable Long accountId);
    
    // 获取账户资产
    @GetMapping("/{accountId}/assets")
    public List<AccountAsset> getAccountAssets(@PathVariable Long accountId);
    
    // 获取账户DEX交易明细
    @GetMapping("/{accountId}/trades")
    public PageResult<DexTradeDetail> getAccountTrades(
        @PathVariable Long accountId,
        @RequestParam(defaultValue = "1") Integer page,
        @RequestParam(defaultValue = "20") Integer size,
        @RequestParam(required = false) String timeRange
    );
    
    // 获取账户转账历史聚合
    @GetMapping("/{accountId}/transfer-history")
    public List<AccountTransferHistory> getTransferHistory(
        @PathVariable Long accountId,
        @RequestParam String timeRange
    );
}
```

## 4. 服务层设计

### 4.1 核心服务接口

```java
@Service
public class TokenService {
    
    // 获取代币信息（PostgreSQL + 随机值生成）
    public TokenInfo getTokenInfo(Long tokenId);
    
    // 获取代币宏观指标（ClickHouse最新数据）
    public TokenMetrics getTokenMetrics(Long tokenId);
    
    // 获取历史价格数据
    public List<TokenPriceHistory> getPriceHistory(Long tokenId, String timeRange);
}

@Service  
public class TradeService {
    
    // 获取交易量统计
    public List<TokenTradeVolume> getTradeVolume(Long tokenId, String timeWindow, String tag);
    
    // 获取净流入数据
    public List<TagNetFlow> getNetFlow(Long tokenId, String timeRange);
    
    // 获取DEX交易明细
    public PageResult<DexTradeDetail> getDexTrades(Long tokenId, Integer page, Integer size, String timeRange);
}

@Service
public class PnLService {
    
    // 获取Top PnL
    public List<TopPnLInfo> getTopPnL(Long tokenId, Integer limit);
    
    // 获取宏观PnL指标
    public MacroPnLMetrics getMacroPnL(Long tokenId);
}

@Service
public class DistributionService {
    
    // 获取代币分布
    public TokenDistribution getTokenDistribution(Long tokenId);
    
    // 获取标签持仓
    public List<TagHolding> getTagHoldings(Long tokenId);
    
    // 获取Top Holder
    public List<TopHolder> getTopHolders(Long tokenId, Integer limit);
}

@Service
public class AccountService {
    
    // 获取账户信息
    public AccountInfo getAccountInfo(Long accountId);
    
    // 获取账户资产
    public List<AccountAsset> getAccountAssets(Long accountId);
    
    // 获取账户交易
    public PageResult<DexTradeDetail> getAccountTrades(Long accountId, Integer page, Integer size, String timeRange);
}
```

## 5. 数据访问层设计

### 5.1 Repository接口

```java
@Repository
public interface TokenRepository {
    
    // PostgreSQL查询
    TokenInfo findTokenById(Long tokenId);
    List<TokenInfo> findAllTokens();
    
    // ClickHouse查询 - 代币指标
    TokenMetrics findLatestMetrics(Long tokenId);
    List<TokenPriceHistory> findPriceHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);
}

@Repository
public interface TradeRepository {
    
    // 交易量统计
    List<TokenTradeVolume> findTradeVolume(Long tokenId, String timeWindow, String tag, LocalDateTime startTime, LocalDateTime endTime);
    
    // 净流入计算
    List<TagNetFlow> calculateNetFlow(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);
    
    // 交易明细
    PageResult<DexTradeDetail> findDexTrades(Long tokenId, Integer offset, Integer limit, LocalDateTime startTime, LocalDateTime endTime);
}

@Repository  
public interface PnLRepository {
    
    // Top PnL查询
    List<TopPnLInfo> findTopPnL(Long tokenId, Integer limit);
    
    // 宏观PnL指标
    MacroPnLMetrics findLatestMacroPnL(Long tokenId);
    List<MacroPnLMetrics> findMacroPnLHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);
}

@Repository
public interface DistributionRepository {
    
    // 分布指标
    TokenDistribution findLatestDistribution(Long tokenId);
    
    // 标签持仓
    List<TagHolding> findTagHoldings(Long tokenId, LocalDateTime endTime);
    List<TagHolding> findTagHoldingsHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);
    
    // Top Holder
    List<TopHolder> findTopHolders(Long tokenId, Integer limit);
}

@Repository
public interface AccountRepository {
    
    // PostgreSQL查询 - 账户基础信息
    AccountInfo findAccountById(Long accountId);
    
    // ClickHouse查询 - 账户资产
    List<AccountAsset> findAccountAssets(Long accountId);
    
    // 账户交易
    PageResult<DexTradeDetail> findAccountTrades(Long accountId, Integer offset, Integer limit, LocalDateTime startTime, LocalDateTime endTime);
    
    // 转账历史
    List<AccountTransferHistory> findTransferHistory(Long accountId, LocalDateTime startTime, LocalDateTime endTime);
}
```

## 6. 工具类设计

### 6.1 标签位图工具

```java
@Component
public class LabelBitmapUtil {
    
    // 标签枚举
    public enum LabelType {
        EXCHANGE(0, "交易所"),
        SMART_MONEY(1, "聪明钱"), 
        WHALE(2, "巨鲸"),
        PUBLIC_FIGURE(3, "公众人物"),
        FRESH_WALLET(4, "新钱包"),
        TOP_PNL(5, "Top PnL");
        
        private final int bit;
        private final String description;
    }
    
    // 解析位图为标签列表
    public List<String> parseLabels(Integer labelMask) {
        if (labelMask == null) return Collections.emptyList();
        
        List<String> labels = new ArrayList<>();
        for (LabelType type : LabelType.values()) {
            if ((labelMask & (1 << type.bit)) != 0) {
                labels.add(type.description);
            }
        }
        return labels;
    }
    
    // 检查是否包含特定标签
    public boolean hasLabel(Integer labelMask, LabelType labelType) {
        if (labelMask == null) return false;
        return (labelMask & (1 << labelType.bit)) != 0;
    }
}
```

### 6.2 时间范围工具

```java
@Component
public class TimeRangeUtil {
    
    // 解析时间范围字符串
    public TimeRange parseTimeRange(String timeRange) {
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime;
        
        switch (timeRange.toLowerCase()) {
            case "1h":
                startTime = endTime.minusHours(1);
                break;
            case "24h":
                startTime = endTime.minusHours(24);
                break;
            case "7d":
                startTime = endTime.minusDays(7);
                break;
            case "30d":
                startTime = endTime.minusDays(30);
                break;
            default:
                startTime = endTime.minusHours(24);
        }
        
        return new TimeRange(startTime, endTime);
    }
    
    @Data
    @AllArgsConstructor
    public static class TimeRange {
        private LocalDateTime startTime;
        private LocalDateTime endTime;
    }
}
```

## 7. 配置类设计

### 7.1 数据库配置

```java
@Configuration
public class DatabaseConfig {
    
    @Primary
    @Bean(name = "clickhouseDataSource")
    @ConfigurationProperties("spring.datasource.clickhouse")
    public DataSource clickhouseDataSource() {
        return DataSourceBuilder.create().build();
    }
    
    @Bean(name = "postgresqlDataSource")
    @ConfigurationProperties("spring.datasource.postgresql") 
    public DataSource postgresqlDataSource() {
        return DataSourceBuilder.create().build();
    }
    
    @Bean(name = "clickhouseJdbcTemplate")
    public JdbcTemplate clickhouseJdbcTemplate(@Qualifier("clickhouseDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    @Bean(name = "postgresqlJdbcTemplate")
    public JdbcTemplate postgresqlJdbcTemplate(@Qualifier("postgresqlDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
```

### 7.2 应用配置

```yaml
spring:
  datasource:
    clickhouse:
      url: jdbc:clickhouse://localhost:8123/default
      username: default
      password: 
      driver-class-name: com.clickhouse.jdbc.ClickHouseDriver
      hikari:
        maximum-pool-size: 10
        minimum-idle: 2
        connection-timeout: 30000
        idle-timeout: 600000
        max-lifetime: 1800000
    
    postgresql:
      url: jdbc:postgresql://localhost:5432/twilight
      username: postgres
      password: password
      driver-class-name: org.postgresql.Driver
      hikari:
        maximum-pool-size: 5
        minimum-idle: 1
        connection-timeout: 30000
        idle-timeout: 600000
        max-lifetime: 1800000

server:
  port: 8080
  servlet:
    context-path: /api

logging:
  level:
    com.twilight.backend: DEBUG
    org.springframework.jdbc.core: DEBUG

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```

## 8. 关键SQL查询示例

### 8.1 代币宏观指标查询

```sql
-- 获取最新代币指标
SELECT 
  token_id,
  end_time,
  token_price_usd as current_price,
  mcap_usd as mcap,
  fdv_usd as fdv,
  liquidity_usd as liquidity,
  fdv_usd / nullIf(mcap_usd, 0) as fdv_mcap_ratio,
  mcap_usd / nullIf(liquidity_usd, 0) as mcap_liquidity_ratio,
  fdv_usd / nullIf(liquidity_usd, 0) as fdv_liquidity_ratio
FROM token_recent_metric_ch 
WHERE token_id = ? 
  AND tag = 'all'
  AND time_window = '1min'
ORDER BY end_time DESC 
LIMIT 1
```

### 8.2 DEX交易明细查询

```sql
-- 获取DEX交易明细
SELECT 
  tx_hash,
  block_time,
  CASE 
    WHEN side = 'BUY' THEN pair_address
    ELSE account_address
  END as from_address,
  CASE 
    WHEN side = 'BUY' THEN account_address  
    ELSE pair_address
  END as to_address,
  token_id,
  side,
  qty,
  price_usd,
  value_usd,
  label_mask
FROM ch_account_trade_fact
WHERE token_id = ?
  AND block_time >= ?
  AND block_time <= ?
ORDER BY block_time DESC
LIMIT ? OFFSET ?
```

### 8.3 Top PnL查询

```sql
-- 获取Top PnL
SELECT 
  p.account_id,
  a.address,
  p.token_id,
  p.total_pnl_usd,
  p.roi_pct,
  p.realized_pnl_usd,
  p.unrealized_pnl_usd,
  p.holding_pct,
  a.tag_bitmap as label_mask
FROM ch_account_pnl_current_ma p
JOIN account a ON p.account_id = a.id  
WHERE p.token_id = ?
ORDER BY p.total_pnl_usd DESC
LIMIT ?
```

### 8.4 账户资产查询

```sql  
-- 获取账户资产
SELECT 
  asset_type,
  biz_id,
  CASE 
    WHEN asset_type = 'erc20' THEN (SELECT token_symbol FROM token WHERE id = biz_id)
    WHEN asset_type = 'lp' THEN 'LP Token'
    ELSE 'ETH'
  END as biz_name,
  amount,
  value_usd,
  price_usd
FROM ch_account_balance_snapshot
WHERE account_id = ?
  AND observed_time = (
    SELECT max(observed_time) 
    FROM ch_account_balance_snapshot 
    WHERE account_id = ?
  )
ORDER BY value_usd DESC
```

## 9. 异常处理和监控

### 9.1 全局异常处理

```java
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleException(Exception e) {
        log.error("系统异常", e);
        return ResponseEntity.status(500)
            .body(new ErrorResponse("SYSTEM_ERROR", "系统异常，请稍后重试"));
    }
    
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException e) {
        return ResponseEntity.status(400)
            .body(new ErrorResponse("INVALID_PARAMETER", e.getMessage()));
    }
}
```

### 9.2 性能监控

```java
@Aspect
@Component
public class PerformanceAspect {
    
    @Around("@annotation(Monitored)")
    public Object monitor(ProceedingJoinPoint pjp) throws Throwable {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        
        try {
            return pjp.proceed();
        } finally {
            stopWatch.stop();
            long executionTime = stopWatch.getTotalTimeMillis();
            
            if (executionTime > 1000) {
                log.warn("慢查询警告: {}ms - {}", executionTime, pjp.getSignature());
            }
        }
    }
}
```

## 10. 部署和运维

### 10.1 Maven配置

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-jdbc</artifactId>
    </dependency>
    <dependency>
        <groupId>com.clickhouse</groupId>
        <artifactId>clickhouse-jdbc</artifactId>
        <version>0.4.6</version>
    </dependency>
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
</dependencies>
```

### 10.2 容器化部署

```dockerfile
FROM openjdk:8-jre-slim
COPY target/backend-1.0.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

这个设计文档完整覆盖了前端所需的所有功能，基于现有的ClickHouse表结构，使用Spring Boot 2.x构建高性能的Web后端服务。
