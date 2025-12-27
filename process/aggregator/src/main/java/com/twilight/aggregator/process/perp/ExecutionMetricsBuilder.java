package com.twilight.aggregator.process.perp;

import java.math.BigDecimal;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.perp.OrderBookSummary;
import com.twilight.aggregator.model.perp.TradesSummary;
import com.twilight.aggregator.model.perp.ExecutionMetrics;

/**
 * 执行面指标构建器
 * 
 * 将OrderBookSummary和TradesSummary连接，生成完整的ExecutionMetrics
 * 使用CoProcessFunction在同一秒窗口内关联两个流
 * 
 * 注意：OFI已在OrderBookProcessor中计算并存储在状态中
 * 这里主要是组装最终输出
 */
public class ExecutionMetricsBuilder 
        extends CoProcessFunction<OrderBookSummary, TradesSummary, ExecutionMetrics> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(ExecutionMetricsBuilder.class);
    
    // 算法版本（用于A/B测试和指标口径变更追溯）
    private static final String ALGO_VERSION = "v1.0";
    
    // 临时存储：使用简单的last-one策略
    // 注意：这里假设OrderBook和Trades在相同窗口内到达
    // 生产环境可使用ConnectedStreams with KeyedCoProcessFunction + ValueState
    private transient OrderBookSummary lastOrderBook;
    private transient TradesSummary lastTrades;
    
    @Override
    public void processElement1(OrderBookSummary orderBook, Context ctx, Collector<ExecutionMetrics> out) 
            throws Exception {
        lastOrderBook = orderBook;
        tryEmit(out);
    }
    
    @Override
    public void processElement2(TradesSummary trades, Context ctx, Collector<ExecutionMetrics> out) 
            throws Exception {
        lastTrades = trades;
        tryEmit(out);
    }
    
    /**
     * 尝试发射ExecutionMetrics
     * 当OrderBook和Trades都到达时发射
     */
    private void tryEmit(Collector<ExecutionMetrics> out) {
        if (lastOrderBook == null) {
            return; // 等待OrderBook
        }
        
        // 即使没有trades也可以发射（trades指标为0）
        if (lastTrades == null) {
            log.debug("No trades for symbol {}, emitting with zero trade metrics", 
                     lastOrderBook.getSymbol());
        }
        
        // 构建ExecutionMetrics
        ExecutionMetrics.ExecutionMetricsBuilder builder = ExecutionMetrics.builder()
                .symbol(lastOrderBook.getSymbol())
                .exchange(lastOrderBook.getExchange())
                .endTime(lastOrderBook.getWindowEnd())
                .algoVersion(ALGO_VERSION)
                
                // 订单簿指标
                .midPrice(lastOrderBook.getMidPrice())
                .spreadBps(lastOrderBook.getSpreadBps() != null ? lastOrderBook.getSpreadBps().doubleValue() : null)
                .spreadAbs(lastOrderBook.getSpreadAbs())
                .depth10k(lastOrderBook.getDepth10k())
                .depth50k(lastOrderBook.getDepth50k())
                .depth100k(lastOrderBook.getDepth100k())
                .imbalanceTop5(lastOrderBook.getImbalanceTop5())
                .imbalanceTotal(lastOrderBook.getImbalanceTotal())
                .impact10kBps(lastOrderBook.getImpact10kBps())
                .impact50kBps(lastOrderBook.getImpact50kBps())
                .impact100kBps(lastOrderBook.getImpact100kBps());
        
        // OFI在OrderBookProcessor中已计算，这里暂时设为0
        // 实际应从OrderBookSummary传递或使用状态
        builder.ofi(0.0); // TODO: 从状态读取OFI
        
        // 成交指标（使用零值代替null，满足ClickHouse非null约束）
        if (lastTrades != null) {
            builder.tradeCount(lastTrades.getTradeCount())
                   .volumeUsd(lastTrades.getVolumeUsd())
                   .vwap(lastTrades.getVwap())
                   .buyVolumeUsd(lastTrades.getBuyVolumeUsd())
                   .sellVolumeUsd(lastTrades.getSellVolumeUsd());
        } else {
            // 无交易窗口：使用零值而非null
            builder.tradeCount(0)
                   .volumeUsd(BigDecimal.ZERO)
                   .vwap(BigDecimal.ZERO)
                   .buyVolumeUsd(BigDecimal.ZERO)
                   .sellVolumeUsd(BigDecimal.ZERO);
        }
        
        // 流动性指标（可选，分钟级更稳定）
        builder.illiqLambda(null);
        
        builder.processTime(System.currentTimeMillis());
        
        ExecutionMetrics metrics = builder.build();
        out.collect(metrics);
        
        // 清空缓存
        lastOrderBook = null;
        lastTrades = null;
        
        if (log.isDebugEnabled()) {
            log.debug("ExecutionMetrics emitted: symbol={}, mid={}, spread={}, volume={}", 
                     metrics.getSymbol(), metrics.getMidPrice(), metrics.getSpreadBps(), 
                     metrics.getVolumeUsd());
        }
    }
}

