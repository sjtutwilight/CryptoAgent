package com.twilight.aggregator.process;

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.util.Collector;

import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.TokenMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 统一的元数据增强算子
 * 整合Pair元数据查询和TokenMetrics广播功能
 * 为ProcessEvent提供完整的元数据信息，包括：
 * - Pair信息 (已通过UnifiedEventProcessor处理)
 * - Token价格和指标信息 (通过Redis广播流)
 * - 地址标签信息 (已通过UnifiedEventProcessor处理)
 */
public class MetadataEnrichmentOperator extends BroadcastProcessFunction<ProcessEvent, Map<String, TokenMetrics>, ProcessEvent> {
    private static final Logger log = LoggerFactory.getLogger(MetadataEnrichmentOperator.class);
    
    public static final MapStateDescriptor<String, TokenMetrics> TOKEN_METRICS_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>("token-metrics", String.class, TokenMetrics.class);
    
    private final boolean requireValidPrices;
    private final boolean requireCompleteMetrics;
    
    // 统计计数器
    private transient long processedEvents = 0;
    private transient long fullyEnrichedEvents = 0;
    private transient long partiallyEnrichedEvents = 0;
    private transient long unenrichedEvents = 0;
    
    // 指标计数器
    private transient long token0PriceHits = 0;
    private transient long token0PriceMisses = 0;
    private transient long token1PriceHits = 0;
    private transient long token1PriceMisses = 0;
    private transient long token0MetricsHits = 0;
    private transient long token0MetricsMisses = 0;
    private transient long token1MetricsHits = 0;
    private transient long token1MetricsMisses = 0;
    
    public MetadataEnrichmentOperator() {
        this(false, false);
    }
    
    public MetadataEnrichmentOperator(boolean requireValidPrices, boolean requireCompleteMetrics) {
        this.requireValidPrices = requireValidPrices;
        this.requireCompleteMetrics = requireCompleteMetrics;
    }
    
    @Override
    public void processElement(
            ProcessEvent event, 
            BroadcastProcessFunction<ProcessEvent, Map<String, TokenMetrics>, ProcessEvent>.ReadOnlyContext ctx, 
            Collector<ProcessEvent> out) throws Exception {
        
        processedEvents++;
        
        try {
            org.apache.flink.api.common.state.ReadOnlyBroadcastState<String, TokenMetrics> metricsState = 
                ctx.getBroadcastState(TOKEN_METRICS_STATE_DESCRIPTOR);
            
            // 增强事件的Token指标信息
            EnrichmentResult result = enrichEventWithTokenMetrics(event, metricsState);
            
            // 统计增强结果
            switch (result) {
                case FULLY_ENRICHED:
                    fullyEnrichedEvents++;
                    break;
                case PARTIALLY_ENRICHED:
                    partiallyEnrichedEvents++;
                    break;
                case UNENRICHED:
                    unenrichedEvents++;
                    break;
            }
            
            // 验证是否满足输出要求
            boolean shouldOutput = true;
            String skipReason = null;
            
            if (requireValidPrices && !hasValidPrices(event)) {
                shouldOutput = false;
                skipReason = "invalid prices";
            } else if (requireCompleteMetrics && result == EnrichmentResult.UNENRICHED) {
                shouldOutput = false;
                skipReason = "missing metrics";
            }
            
            if (shouldOutput) {
                out.collect(event);
                log.trace("✅ Enriched and collected event: contract={}, result={}", 
                         event.getContractAddress(), result);
            } else {
                log.trace("🚫 Skipping event due to {}: contract={}", 
                         skipReason, event.getContractAddress());
            }
            
            // 每1000个事件记录一次统计
            if (processedEvents % 1000 == 0) {
                logStatistics();
            }
            
        } catch (Exception e) {
            log.error("💥 Error enriching event for contract {}: {}", 
                     event.getContractAddress(), e.getMessage(), e);
            // 即使出错也输出原始事件，保证数据流的连续性
            out.collect(event);
        }
    }
    
    /**
     * 使用Token指标信息增强事件
     */
    private EnrichmentResult enrichEventWithTokenMetrics(
            ProcessEvent event, 
            org.apache.flink.api.common.state.ReadOnlyBroadcastState<String, TokenMetrics> metricsState) 
            throws Exception {
        
        boolean token0Enriched = false;
        boolean token1Enriched = false;
        
        // 增强token0信息
        if (event.getToken0Address() != null) {
            String token0Key = event.getToken0Address().toLowerCase();
            TokenMetrics token0Metrics = metricsState.get(token0Key);
            
            if (token0Metrics != null) {
                // 设置价格信息
                if (token0Metrics.hasPrice()) {
                    event.setToken0PriceUsd(token0Metrics.getTokenPriceUsd());
                    token0PriceHits++;
                    token0Enriched = true;
                    log.trace("💰 Enriched token0 price: {} = ${}", token0Key, token0Metrics.getTokenPriceUsd());
                } else {
                    token0PriceMisses++;
                    log.trace("⚠️ Token0 missing price: {}", token0Key);
                }
                
                // 设置其他指标信息
                if (token0Metrics.hasAllMetrics()) {
                    event.setToken0Mcap(token0Metrics.getMcap());
                    event.setToken0Fdv(token0Metrics.getFdv());
                    event.setToken0LiquidityUsd(token0Metrics.getLiquidityUsd());
                    token0MetricsHits++;
                    
                    log.trace("📊 Enriched token0 metrics: {} - mcap=${}, fdv=${}, liquidity=${}", 
                             token0Key, token0Metrics.getMcap(), token0Metrics.getFdv(), token0Metrics.getLiquidityUsd());
                } else {
                    token0MetricsMisses++;
                    log.trace("❓ Token0 incomplete metrics: {}", token0Key);
                }
            } else {
                token0PriceMisses++;
                token0MetricsMisses++;
                log.trace("❓ No metrics found for token0: {}", token0Key);
            }
        }
        
        // 增强token1信息
        if (event.getToken1Address() != null) {
            String token1Key = event.getToken1Address().toLowerCase();
            TokenMetrics token1Metrics = metricsState.get(token1Key);
            
            if (token1Metrics != null) {
                // 设置价格信息
                if (token1Metrics.hasPrice()) {
                    event.setToken1PriceUsd(token1Metrics.getTokenPriceUsd());
                    token1PriceHits++;
                    token1Enriched = true;
                    log.trace("💰 Enriched token1 price: {} = ${}", token1Key, token1Metrics.getTokenPriceUsd());
                } else {
                    token1PriceMisses++;
                    log.trace("⚠️ Token1 missing price: {}", token1Key);
                }
                
                // 设置其他指标信息
                if (token1Metrics.hasAllMetrics()) {
                    event.setToken1Mcap(token1Metrics.getMcap());
                    event.setToken1Fdv(token1Metrics.getFdv());
                    event.setToken1LiquidityUsd(token1Metrics.getLiquidityUsd());
                    token1MetricsHits++;
                    
                    log.trace("📊 Enriched token1 metrics: {} - mcap=${}, fdv=${}, liquidity=${}", 
                             token1Key, token1Metrics.getMcap(), token1Metrics.getFdv(), token1Metrics.getLiquidityUsd());
                } else {
                    token1MetricsMisses++;
                    log.trace("❓ Token1 incomplete metrics: {}", token1Key);
                }
            } else {
                token1PriceMisses++;
                token1MetricsMisses++;
                log.trace("❓ No metrics found for token1: {}", token1Key);
            }
        }
        
        // 根据增强结果返回状态
        if (token0Enriched && token1Enriched) {
            return EnrichmentResult.FULLY_ENRICHED;
        } else if (token0Enriched || token1Enriched) {
            return EnrichmentResult.PARTIALLY_ENRICHED;
        } else {
            return EnrichmentResult.UNENRICHED;
        }
    }
    
    /**
     * 检查事件是否有有效的价格信息
     */
    private boolean hasValidPrices(ProcessEvent event) {
        return event.getToken0PriceUsd() > 0 && event.getToken1PriceUsd() > 0;
    }
    
    /**
     * 记录统计信息
     */
    private void logStatistics() {
        double fullyEnrichedRate = (double) fullyEnrichedEvents / processedEvents * 100;
        double partiallyEnrichedRate = (double) partiallyEnrichedEvents / processedEvents * 100;
        double unenrichedRate = (double) unenrichedEvents / processedEvents * 100;
        
        log.info("📊 MetadataEnrichment Stats - Processed: {}", processedEvents);
        log.info("   ✅ Fully enriched: {} ({:.1f}%)", fullyEnrichedEvents, fullyEnrichedRate);
        log.info("   🔶 Partially enriched: {} ({:.1f}%)", partiallyEnrichedEvents, partiallyEnrichedRate);
        log.info("   ❌ Unenriched: {} ({:.1f}%)", unenrichedEvents, unenrichedRate);
        log.info("   💰 Price hits: token0={}, token1={}", token0PriceHits, token1PriceHits);
        log.info("   ❓ Price misses: token0={}, token1={}", token0PriceMisses, token1PriceMisses);
        log.info("   📊 Metrics hits: token0={}, token1={}", token0MetricsHits, token1MetricsHits);
        log.info("   ❓ Metrics misses: token0={}, token1={}", token0MetricsMisses, token1MetricsMisses);
    }
    
    @Override
    public void processBroadcastElement(
            Map<String, TokenMetrics> metricsUpdate, 
            BroadcastProcessFunction<ProcessEvent, Map<String, TokenMetrics>, ProcessEvent>.Context ctx, 
            Collector<ProcessEvent> out) throws Exception {
        
        BroadcastState<String, TokenMetrics> metricsState = ctx.getBroadcastState(TOKEN_METRICS_STATE_DESCRIPTOR);
        
        int updatedCount = 0;
        int priceOnlyCount = 0;
        int fullMetricsCount = 0;
        
        try {
            for (Map.Entry<String, TokenMetrics> entry : metricsUpdate.entrySet()) {
                String tokenAddress = entry.getKey().toLowerCase();
                TokenMetrics metrics = entry.getValue();
                
                // 更新广播状态
                metricsState.put(tokenAddress, metrics);
                updatedCount++;
                
                // 统计指标类型
                if (metrics.hasAllMetrics()) {
                    fullMetricsCount++;
                    log.trace("📊 Broadcast full metrics: {} - price=${}, mcap=${}, fdv=${}, liquidity=${}", 
                             tokenAddress, metrics.getTokenPriceUsd(), metrics.getMcap(), 
                             metrics.getFdv(), metrics.getLiquidityUsd());
                } else if (metrics.hasPrice()) {
                    priceOnlyCount++;
                    log.trace("💰 Broadcast price only: {} = ${}", tokenAddress, metrics.getTokenPriceUsd());
                }
            }
            
            log.info("🔄 Broadcasted {} token metrics (full: {}, price-only: {})", 
                    updatedCount, fullMetricsCount, priceOnlyCount);
            
        } catch (Exception e) {
            log.error("💥 Error broadcasting token metrics: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 增强结果枚举
     */
    private enum EnrichmentResult {
        /** 两个token都成功增强了价格信息 */
        FULLY_ENRICHED,
        /** 只有一个token成功增强了价格信息 */
        PARTIALLY_ENRICHED,
        /** 没有任何token增强成功 */
        UNENRICHED
    }
    
    /**
     * 工厂方法
     */
    public static class Factory {
        /**
         * 创建标准的元数据增强算子 - 不强制要求价格和指标
         */
        public static MetadataEnrichmentOperator createStandard() {
            return new MetadataEnrichmentOperator(false, false);
        }
        
        /**
         * 创建严格的元数据增强算子 - 要求有效价格
         */
        public static MetadataEnrichmentOperator createWithRequiredPrices() {
            return new MetadataEnrichmentOperator(true, false);
        }
        
        /**
         * 创建完整的元数据增强算子 - 要求完整指标
         */
        public static MetadataEnrichmentOperator createWithCompleteMetrics() {
            return new MetadataEnrichmentOperator(false, true);
        }
        
        /**
         * 创建严格完整的元数据增强算子 - 要求有效价格和完整指标
         */
        public static MetadataEnrichmentOperator createStrict() {
            return new MetadataEnrichmentOperator(true, true);
        }
    }
}
