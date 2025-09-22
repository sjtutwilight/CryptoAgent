
0) 总体 DAG（每个 Job 内的标准前置子图）
KafkaSource<byte[]>("dex_transaction")
 → HeaderFastFilter              // 仅解析 eventName/基本头信息，做早过滤
 → EventExtractor                // 仅对通过过滤的记录做强类型解析：ParsedSwap/Transfer/Mint/Burn
 → AsyncMetaEnrich               // 地址→ID、decimals、label、精度统一（异步 + 本地缓存）
 → PriceBroadcastJoin            // 价格/市值等广播态补齐（BroadcastProcessFunction）
 → BranchToJobSpecificStreams    // map/side-output 到各 Job 需要的域模型
 → 下游各 Job 主处理算子


解析/增强只做一次；过滤放最前面；分支在广播之后（因为大多数 Job 都需要价格/精度）。

1) 早过滤：HeaderFastFilter（极轻量）

目的：不为不需要的事件付出“ABI 完整解析 + 外部 IO”成本。
做法：用 Jackson streaming 只扫 eventName、txHash、logIndex、blockNumber，其他字段不读。

public final class HeaderFastFilter extends RichFlatMapFunction<byte[], RawEnvelope> {
  static final ObjectMapper M = new ObjectMapper(); // streaming
  private final FilterType filterType;
  private final Set<String> allow, block;

  public void flatMap(byte[] value, Collector<RawEnvelope> out) throws Exception {
    // 仅解析顶层 needed 字段（eventName 在 events[i] 中）
    // 一旦命中 block 或不在 allow，直接丢弃；命中则带着最小头部进入下一步
    RawEnvelope env = parseHeaderOnly(value); // chainId, txHash, blockId, timeMs, eventName, contractAddr, logIndex …
    if (!isAllowed(env.eventName())) return;
    out.collect(env);
  }
}


FilterType = SWAP_ONLY | BALANCE_EVENTS | ALL_DEX_EVENTS | CUSTOM（你已有）。

这一步不解码 decodedArgs，纯 CPU、极快。

2) 强类型解析：EventExtractor（只对已筛选事件）

输出四个不可变强类型事件（避免 Map/Object）：

sealed interface ParsedDexEvent permits ParsedSwap, ParsedTransfer, ParsedMint, ParsedBurn {
  EventHeader header(); DexEventType type();
}

record ParsedSwap(EventHeader h,
  String token0Addr, String token1Addr,
  BigDecimal amount0In, BigDecimal amount1In,
  BigDecimal amount0Out, BigDecimal amount1Out,
  String sender, String to) implements ParsedDexEvent { public DexEventType type(){return SWAP;} }

record ParsedTransfer(EventHeader h, String tokenAddr, String from, String to, BigDecimal amount)
  implements ParsedDexEvent { public DexEventType type(){return TRANSFER;} }

record ParsedMint(EventHeader h, String token0Addr, String token1Addr,
  BigDecimal amount0, BigDecimal amount1, String sender, String to)
  implements ParsedDexEvent { public DexEventType type(){return MINT;} }

record ParsedBurn(EventHeader h, String token0Addr, String token1Addr,
  BigDecimal amount0, BigDecimal amount1, String sender, String to)
  implements ParsedDexEvent { public DexEventType type(){return BURN;} }


统一 BigDecimal，不要用 double（你的 Token/Pair 里原来是 double，容易精度漂移）。

EventHeader 包含 chainId, blockId, blockTimeMs, txHash, logIndex, contract/pairAddr。

3) 异步增强：AsyncMetaEnrich（地址 → ID / 精度 / 标签）

目的：一次性补齐所有 Job 都要用的“稳定维度”，并缓存。
建议：

Flink AsyncFunction<ParsedDexEvent, EnrichedDexEvent>

读 Redis（或内存+本地 Caffeine 缓存）获取：tokenId/decimals、pairId(token0,1)、accountId(from/to/sender)、label_mask…

幂等 & 限流：超时降级为“unknown id”，并打指标；缓存 TTL ≥ 1h。

输出四类 Enriched* 事件（ID/精度就绪）：

sealed interface EnrichedDexEvent permits EnrichedSwap, EnrichedTransfer, EnrichedMint, EnrichedBurn {
  EventHeader header(); DexEventType type(); Map<String,Object> labels();
}

record EnrichedSwap(EventHeader h, long pairId, long token0Id, long token1Id,
  BigDecimal amount0In, BigDecimal amount1In, BigDecimal amount0Out, BigDecimal amount1Out,
  Long traderAccountId, String sender, String to, Map<String,Object> labels) implements EnrichedDexEvent { … }

record EnrichedTransfer(EventHeader h, long tokenId, long fromAccountId, long toAccountId,
  BigDecimal amount, Map<String,Object> labels) implements EnrichedDexEvent { … }

record EnrichedMint(EventHeader h, long pairId, long token0Id, long token1Id,
  BigDecimal amount0, BigDecimal amount1, Long providerAccountId, Map<String,Object> labels) implements EnrichedDexEvent { … }

record EnrichedBurn(… 同上 …) implements EnrichedDexEvent { … }


这一步不做价格换算，只做“谁/什么东西”的确定。

4) 价格广播：PriceBroadcastJoin（无 key 主流 ⟂ 广播流）

广播流：PriceTick{tokenId, priceUsd, mcapUsd, fdvUsd, liquidityUsd, asOfMs}

用 BroadcastProcessFunction<EnrichedDexEvent, PriceTick, EnrichedDexEventWithPx>

结果事件追加价格/价值字段（仍然BigDecimal）

record EnrichedDexEventWithPx<T extends EnrichedDexEvent>(
  T base,
  Map<Long, PriceSnapshot> px // 可含 1 或 2 个 token 的价
) {}


对 SWAP 同时取 token0 与 token1 的价格；

对 TRANSFER/MINT/BURN 取单 token 价格；

没价格则 valueUsd 后续填 0 并标记 px_missing=true 指标。

5) 分支到各 Job 的域模型（你给的四类对象，做了优化）
5.1 AccountTrade（PnL Job 输入）
public record AccountTrade(
  long accountId,
  String accountAddress,     // 可选
  long tokenId,
  String tokenAddress,       // 可选
  Side side,                 // BUY/SELL
  BigDecimal quantity,       // 原生单位
  BigDecimal priceUsd,       // 单价
  BigDecimal valueUsd,       // = qty * priceUsd
  long blockId,
  long blockTimeMs,
  String txHash,
  int logIndex,
  int labelMask              // 建议位图，不再用 String tag
) implements Serializable {
  public enum Side { BUY, SELL }
}


映射要点（从 EnrichedSwap + Px）：

依据你的口径判定主标的与 Side（例如“拿到的 token 为 BUY”）；

priceUsd = valueUsd/qty（qty=0 时置 0）；

统一 BigDecimal 精度（数量 scale=18，USD scale=8/4），落库前 setScale()。

5.2 BalanceDelta（资产快照/Top Holder 输入）
public record BalanceDelta(
  long accountId,
  String accountAddress,  // 可选
  AssetType assetType,    // ERC20/LP
  long bizId,             // token_id 或 pair_id
  String contractAddress, // 可选
  BigDecimal delta,       // +增 -减
  long eventTimeMs,
  long blockId,
  String txHash,
  BalanceEventType eventType, // TRANSFER_IN/OUT/MINT/BURN
  BigDecimal priceUsd,
  int labelMask
) implements Serializable {
  public enum AssetType { ERC20, LP }
  public enum BalanceEventType { TRANSFER_IN, TRANSFER_OUT, MINT, BURN }
}


映射：

Transfer 生成两条：from → OUT(-delta)；to → IN(+delta)；

Mint/Burn 对 token0/token1 分别生成 ERC20 delta（或单独 LP 表，取决于你的资产口径）。

5.3 Token / Pair（用于你的指标 Job）

你给的 Token/Pair 把 double → BigDecimal，时间统一 long ms，并去掉不必常驻的指标字段（mcap/fdv/liquidity 通过广播取用，不嵌在事件里）：

public record TokenEvent(
  long tokenId, String tokenAddress,
  long accountId, String fromAddress, int labelMask,
  BigDecimal amount, BigDecimal priceUsd,
  boolean buyOrSell, // 若你确需保留布尔
  long timestampMs
) {}

public record PairEvent(
  long pairId, String pairAddress,
  long token0Id, long token1Id,
  BigDecimal amount0In, BigDecimal amount0Out,
  BigDecimal amount1In, BigDecimal amount1Out,
  BigDecimal token0PriceUsd, BigDecimal token1PriceUsd,
  String eventName,
  long timestampMs, String fromAddress
) {}




**token滑动窗口**

先聚合最小时间粒度(20s)的窗口结果，以20s为滑动步长,高层级窗口以20s聚合结果进行增量聚合。

按tag聚合:除了全量聚合外，按照token流中tag标签进行聚合，对应表中tag字段。tag从

### **状态**

MapState<String,TokenRecentMetric> 20sMetricHistory

key:timestamp+_+tag, 存储20s窗口的历史值,状态保留时间：70分钟

MapState<String,TokenRecentMetric> recentMetric

key: {”1min”,”5min”,”1h”}+_+tag,存储高层级窗口上一次聚合值

### **增量聚合逻辑**

若当前时间戳为t,窗口宽度为p

newMetric=20sMetricHistory(t)-20sMetricHistory(t-p)+recentMetric

## **pair处理**

keyBy:contractAddress

多个滚动时间窗口，窗口时间为20s,1min,5min,1h

## **tokenPriceUsd**

以twswap_pair表中各token 与usdc的pair的reserve比值为usd价格,usdc价格为1。reserve从twswap_pair_metric最新的数据获取

比如weth是token0,usdc是token1,weth的tokenPriceUsd为token1_reserve/token0_reserve，反之同理

redis key:tokenAddress value:tokenPriceUsd

### **指标计算**

下面指标为窗口期累加值，初始值为0：

- token0_volume_usd
- token1_volume_usd
- volume_usd
- txcnt

下面指标在窗口内更新值：

- token0_reserve
- token1_reserve
- reserve_usd

**事件处理逻辑**

### **Sync:**

- 更新 token0_reserve, token1_reserve，如果token中包含usdc,则更新redis中pair另一种token的tokenPriceUsd
- 计算并更新 reserve_usd = token0_reserve * token0PriceUsd + token1_reserve * token1PriceUsd
- 增加 txcnt

### **Swap:**

- 更新 token0_volume_usd += amount0In * token0PriceUsd + amount0Out * token0PriceUsd
- 更新 token1_volume_usd 同理
- 增加 txcnt

### **Mint/Burn:**

- 仅增加 txcnt