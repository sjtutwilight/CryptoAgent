package com.twilight.aggregator.process.pnl;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.AccountTrade;
import com.twilight.aggregator.model.AccountPnLSnapshot;
import com.twilight.aggregator.model.PnLState;
import com.twilight.aggregator.model.PnLRealizedEvent;
import com.twilight.aggregator.model.TokenMetrics;
import com.twilight.aggregator.process.common.RedisTokenMetricsBroadcaster;
/**
 * PnL处理器：核心的移动平均成本算法和状态管理
 * 基于KeyedBroadcastProcessFunction实现，支持价格广播流
 */
public class PnLProcessor extends KeyedBroadcastProcessFunction<String, AccountTrade, Map<String, TokenMetrics>, AccountPnLSnapshot> {
    private static final Logger log = LoggerFactory.getLogger(PnLProcessor.class);
    
    // 侧输出标签：已实现盈亏事件
    public static final OutputTag<PnLRealizedEvent> REALIZED_EVENT_TAG = new OutputTag<PnLRealizedEvent>("realized-events"){};
    
    // 状态描述符
    private static final ValueStateDescriptor<PnLState> PNL_STATE_DESCRIPTOR = 
        new ValueStateDescriptor<>("pnl-state", PnLState.class);
    
    // 价格广播状态描述符 - 与RedisTokenMetricsBroadcaster保持一致
    // 复用RedisTokenMetricsBroadcaster的状态描述符，确保一致性
    public static final MapStateDescriptor<String, TokenMetrics> TOKEN_PRICE_STATE_DESCRIPTOR = 
        RedisTokenMetricsBroadcaster.TOKEN_METRICS_STATE_DESCRIPTOR;
    
    // 状态存储
    private transient ValueState<PnLState> pnlState;
    
    // 统计计数器
    private transient long processedTrades = 0;
    private transient long buyTrades = 0;
    private transient long sellTrades = 0;
    private transient long snapshotsGenerated = 0;
    private transient long errorCount = 0;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化状态
        pnlState = getRuntimeContext().getState(PNL_STATE_DESCRIPTOR);
        
        log.info("🔧 PnLProcessor initialized for account-token PnL calculation");
    }
    
    @Override
    public void processElement(AccountTrade trade, 
                             KeyedBroadcastProcessFunction<String, AccountTrade, Map<String, TokenMetrics>, AccountPnLSnapshot>.ReadOnlyContext ctx, 
                             Collector<AccountPnLSnapshot> out) throws Exception {
        processedTrades++;
        
        try {
            // 获取或创建PnL状态
            PnLState state = pnlState.value();
            if (state == null) {
                state = initializeAccountTokenState(trade.getAccountId(), trade.getTokenId());
                log.info("🆕 Initialized PnL state for account {} token {}: position={}, avgCost=${}", 
                         trade.getAccountId(), trade.getTokenId(), state.getPosition(), state.getAvgCost());
            }
            
            // 根据交易类型处理状态更新
            if (trade.isBuy()) {
                state.processBuy(trade.getQuantity(), trade.getPriceUsd(), trade.getBlockTimeMs());
                buyTrades++;
     
            } else if (trade.isSell()) {
                PnLState.SellResult sellResult = state.processSell(trade.getQuantity(), trade.getPriceUsd(), trade.getBlockTimeMs());
                sellTrades++;

                // 如果有已实现盈亏，发送事件到侧输出
                if (sellResult != null && sellResult.hasRealized()) {
                    PnLRealizedEvent realizedEvent = PnLRealizedEvent.create(
                        trade.getTokenId(), 
                        trade.getAccountId(),
                        trade.getBlockId(),
                        java.time.LocalDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli(trade.getBlockTimeMs()), 
                            java.time.ZoneOffset.UTC),
                        sellResult.getRealizedQty(),
                        sellResult.getRealizedCostUsd(),
                        sellResult.getRealizedProceedsUsd(),
                        sellResult.getRealizedPnLUsd()
                    );
                    log.info("🔧 realizedEvent: {}", realizedEvent);
                    if (realizedEvent.isValid()) {
                        ctx.output(REALIZED_EVENT_TAG, realizedEvent);

                    }
                }
            } else {
                log.warn("⚠️ Unknown trade side: {}", trade.getSide());
                errorCount++;
                return;
            }
            
            // 保存更新后的状态
            pnlState.update(state);
            // 获取当前价格用于未实现盈亏计算
            BigDecimal currentPrice = getCurrentTokenPrice(ctx, trade.getTokenAddress());
            
            // 生成PnL快照
            AccountPnLSnapshot snapshot = generateSnapshot(trade, state, currentPrice);
            if (snapshot != null && snapshot.isValid()) {
                out.collect(snapshot);
                snapshotsGenerated++;
            } else {
                log.warn("⚠️ Failed to generate valid PnL snapshot for trade: {}_{}", trade.getAccountAddress(), trade.getTokenAddress());
                errorCount++;
            }
            
        } catch (Exception e) {
            log.error("💥 Error processing account trade: {}", e.getMessage(), e);
            errorCount++;
        }
        
        // 每1000笔交易记录一次统计
        if (processedTrades % 1000 == 0) {
            log.info("📊 PnL processing stats - Processed: {}, Buys: {}, Sells: {}, Snapshots: {}, Errors: {}", 
                    processedTrades, buyTrades, sellTrades, snapshotsGenerated, errorCount);
        }
    }
    
    @Override
    public void processBroadcastElement(Map<String, TokenMetrics> tokenMetricsMap, 
                                      KeyedBroadcastProcessFunction<String, AccountTrade, Map<String, TokenMetrics>, AccountPnLSnapshot>.Context ctx, 
                                      Collector<AccountPnLSnapshot> out) throws Exception {
        // 更新价格广播状态
        BroadcastState<String, TokenMetrics> broadcastState = ctx.getBroadcastState(TOKEN_PRICE_STATE_DESCRIPTOR);
        
        for (Map.Entry<String, TokenMetrics> entry : tokenMetricsMap.entrySet()) {
            broadcastState.put(entry.getKey(), entry.getValue());
        }
        
        log.trace("🔄 Updated token price broadcast state with {} tokens", tokenMetricsMap.size());
    }
    
    /**
     * 初始化账户-代币状态，模拟mint为初始价格买入
     * 所有账户初始持有各代币100000个
     */
    private PnLState initializeAccountTokenState(Long accountId, Long tokenId) {
        PnLState state = new PnLState();
        
        // 初始持仓数量
        BigDecimal initialPosition = new BigDecimal("100000");
        
        // 根据tokenId获取初始价格
        BigDecimal initialPrice = getInitialTokenPrice(tokenId);
        
        // 模拟mint为初始价格买入
        state.processBuy(initialPosition, initialPrice, System.currentTimeMillis());
        
        log.info("💰 Initialized account {} with {} {} at ${} each", 
                accountId, initialPosition, getTokenSymbol(tokenId), initialPrice);
        
        return state;
    }
    
    /**
     * 获取token初始价格
     */
    private BigDecimal getInitialTokenPrice(Long tokenId) {
        switch (tokenId.intValue()) {
            case 1: // USDC
                return new BigDecimal("1");
            case 2: // WETH
                return new BigDecimal("3000");
            case 3: // DAI
                return new BigDecimal("1");
            case 4: // TWI
                return new BigDecimal("50");
            case 5: // WBTC
                return new BigDecimal("120000");
            default:
                log.warn("⚠️ Unknown token ID {}, using price $1", tokenId);
                return BigDecimal.ONE;
        }
    }
    
    /**
     * 获取token符号（用于日志）
     */
    private String getTokenSymbol(Long tokenId) {
        switch (tokenId.intValue()) {
            case 1: return "USDC";
            case 2: return "WETH";
            case 3: return "DAI";
            case 4: return "TWI";
            case 5: return "WBTC";
            default: return "UNKNOWN";
        }
    }

    /**
     * 获取当前token价格
     */
    private BigDecimal getCurrentTokenPrice(KeyedBroadcastProcessFunction<String, AccountTrade, Map<String, TokenMetrics>, AccountPnLSnapshot>.ReadOnlyContext ctx, 
                                          String tokenAddress) {
        try {
            if (tokenAddress == null || tokenAddress.trim().isEmpty()) {
                return BigDecimal.ZERO;
            }
            
            ReadOnlyBroadcastState<String, TokenMetrics> broadcastState = ctx.getBroadcastState(TOKEN_PRICE_STATE_DESCRIPTOR);
            // 使用小写的token地址作为key
            String key = tokenAddress.toLowerCase();
            TokenMetrics metrics = broadcastState.get(key);
            
            if (metrics != null && metrics.getTokenPriceUsd() != null && metrics.getTokenPriceUsd() > 0) {
                return BigDecimal.valueOf(metrics.getTokenPriceUsd());
            }
            
            log.debug("🔍 No price found for token {}, using zero", key);
            return BigDecimal.ZERO;
            
        } catch (Exception e) {
            log.warn("⚠️ Failed to get current price for token {}: {}", tokenAddress, e.getMessage());
            return BigDecimal.ZERO;
        }
    }
    
    /**
     * 生成PnL快照
     */
    private AccountPnLSnapshot generateSnapshot(AccountTrade trade, PnLState state, BigDecimal currentPrice) {
        try {
            // 计算各项指标
            BigDecimal unrealizedPnL = state.calculateUnrealizedPnL(currentPrice);
            BigDecimal totalPnL = state.calculateTotalPnL(currentPrice);
            double roiPct = state.calculateROI(currentPrice);
            double holdingPct = state.calculateHoldingPercentage();
            
            // 使用区块时间作为版本号 (转换为Long)
            Long version = trade.getBlockTimeMs() != null ? trade.getBlockTimeMs() / 1000 : System.currentTimeMillis() / 1000;
            
            // 创建快照对象
            AccountPnLSnapshot snapshot = AccountPnLSnapshot.fromTimestamp(
                trade.getAccountId(),
                trade.getAccountAddress(),
                trade.getTokenId(), 
                state.getPosition(),
                state.getAvgCost(),
                state.getRealizedCost(),
                state.getRealizedProceeds(),
                state.getRealizedPnL(),
                currentPrice,
                unrealizedPnL,
                totalPnL,
                roiPct,
                holdingPct,
                state.getLastTxTime(),
                version
            );
            return snapshot;
            
        } catch (Exception e) {
            log.error("💥 Error generating PnL snapshot: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 生成状态键：accountId_tokenId
     */
    public static String generateStateKey(Long accountId, Long tokenId) {
        return String.format("%d_%d", accountId != null ? accountId : 0L, tokenId != null ? tokenId : 0L);
    }
    
    /**
     * 生成状态键：accountAddress_tokenAddress (临时方案，直到accountId可用)
     */
    public static String generateStateKey(String accountAddress, String tokenAddress) {
        return String.format("%s_%s", 
                           accountAddress != null ? accountAddress.toLowerCase() : "unknown",
                           tokenAddress != null ? tokenAddress.toLowerCase() : "unknown");
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        log.info("🛑 PnLProcessor closed. Final stats - Processed: {}, Buys: {}, Sells: {}, Snapshots: {}, Errors: {}", 
                processedTrades, buyTrades, sellTrades, snapshotsGenerated, errorCount);
    }
}

