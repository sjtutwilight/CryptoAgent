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
 * 价格广播处理器 - 专门处理Token价格广播
 * 简化的价格增强算子，只负责Token价格和指标的广播增强
 */
public class PriceBroadcastProcessor extends BroadcastProcessFunction<ProcessEvent, Map<String, TokenMetrics>, ProcessEvent> {
    private static final Logger log = LoggerFactory.getLogger(PriceBroadcastProcessor.class);
    
    public static final MapStateDescriptor<String, TokenMetrics> TOKEN_METRICS_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>("token-metrics", String.class, TokenMetrics.class);
    
    private final boolean requireValidPrices;
    
    // 统计计数器
    private transient long processedEvents = 0;
    private transient long enrichedEvents = 0;
    private transient long token0PriceHits = 0;
    private transient long token0PriceMisses = 0;
    private transient long token1PriceHits = 0;
    private transient long token1PriceMisses = 0;
    
    public PriceBroadcastProcessor(boolean requireValidPrices) {
        this.requireValidPrices = requireValidPrices;
    }
    
    public PriceBroadcastProcessor() {
        this(false);
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
            
            // 增强Token价格信息
            boolean enriched = enrichWithTokenPrices(event, metricsState);
            
            if (enriched) {
                enrichedEvents++;
            }
            
            // 验证是否满足输出要求
            if (requireValidPrices && !hasValidPrices(event)) {
                log.trace("🚫 Event rejected due to missing valid prices: token0={}, token1={}", 
                         event.getToken0PriceUsd(), event.getToken1PriceUsd());
                return;
            }
            
            out.collect(event);
            
            // 每1000个事件记录一次统计
            if (processedEvents % 1000 == 0) {
                logStatistics();
            }
            
        } catch (Exception e) {
            log.error("💥 Error in price broadcast processing: {}", e.getMessage(), e);
            // 即使出错也输出原始事件
            out.collect(event);
        }
    }
    
    /**
     * 使用Token价格增强事件
     */
    private boolean enrichWithTokenPrices(
            ProcessEvent event, 
            org.apache.flink.api.common.state.ReadOnlyBroadcastState<String, TokenMetrics> metricsState) 
            throws Exception {
        
        boolean enriched = false;
        
        // 增强token0价格
        if (event.getToken0Address() != null) {
            String token0Key = event.getToken0Address().toLowerCase();
            TokenMetrics token0Metrics = metricsState.get(token0Key);
            
            if (token0Metrics != null && token0Metrics.hasPrice()) {
                event.setToken0PriceUsd(token0Metrics.getTokenPriceUsd());
                
                // 设置其他指标
                if (token0Metrics.hasAllMetrics()) {
                    event.setToken0Mcap(token0Metrics.getMcap());
                    event.setToken0Fdv(token0Metrics.getFdv());
                    event.setToken0LiquidityUsd(token0Metrics.getLiquidityUsd());
                }
                
                token0PriceHits++;
                enriched = true;
                log.trace("💰 Enriched token0 price: {} = ${}", token0Key, token0Metrics.getTokenPriceUsd());
            } else {
                token0PriceMisses++;
                log.trace("❓ No price for token0: {}", token0Key);
            }
        }
        
        // 增强token1价格
        if (event.getToken1Address() != null) {
            String token1Key = event.getToken1Address().toLowerCase();
            TokenMetrics token1Metrics = metricsState.get(token1Key);
            
            if (token1Metrics != null && token1Metrics.hasPrice()) {
                event.setToken1PriceUsd(token1Metrics.getTokenPriceUsd());
                
                // 设置其他指标
                if (token1Metrics.hasAllMetrics()) {
                    event.setToken1Mcap(token1Metrics.getMcap());
                    event.setToken1Fdv(token1Metrics.getFdv());
                    event.setToken1LiquidityUsd(token1Metrics.getLiquidityUsd());
                }
                
                token1PriceHits++;
                enriched = true;
                log.trace("💰 Enriched token1 price: {} = ${}", token1Key, token1Metrics.getTokenPriceUsd());
            } else {
                token1PriceMisses++;
                log.trace("❓ No price for token1: {}", token1Key);
            }
        }
        
        return enriched;
    }
    
    /**
     * 检查是否有有效价格
     */
    private boolean hasValidPrices(ProcessEvent event) {
        return event.getToken0PriceUsd() > 0 && event.getToken1PriceUsd() > 0;
    }
    
    /**
     * 记录统计信息
     */
    private void logStatistics() {
        double enrichmentRate = processedEvents > 0 ? (double) enrichedEvents / processedEvents * 100 : 0;
        
        log.info("📊 PriceBroadcast Stats:");
        log.info("   📨 Processed events: {}", processedEvents);
        log.info("   💰 Enriched events: {} ({:.1f}%)", enrichedEvents, enrichmentRate);
        log.info("   🎯 Token0 price: hits={}, misses={}", token0PriceHits, token0PriceMisses);
        log.info("   🎯 Token1 price: hits={}, misses={}", token1PriceHits, token1PriceMisses);
    }
    
    @Override
    public void processBroadcastElement(
            Map<String, TokenMetrics> metricsUpdate,
            BroadcastProcessFunction<ProcessEvent, Map<String, TokenMetrics>, ProcessEvent>.Context ctx,
            Collector<ProcessEvent> out) throws Exception {
        
        BroadcastState<String, TokenMetrics> metricsState = ctx.getBroadcastState(TOKEN_METRICS_STATE_DESCRIPTOR);
        
        int updatedCount = 0;
        try {
            for (Map.Entry<String, TokenMetrics> entry : metricsUpdate.entrySet()) {
                String tokenAddress = entry.getKey().toLowerCase();
                TokenMetrics metrics = entry.getValue();
                metricsState.put(tokenAddress, metrics);
                updatedCount++;
            }
            
            log.info("🔄 Broadcasted {} token metrics", updatedCount);
            
        } catch (Exception e) {
            log.error("💥 Error broadcasting token metrics: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 工厂方法
     */
    public static class Factory {
        /**
         * 创建标准的价格广播处理器
         */
        public static PriceBroadcastProcessor createStandard() {
            return new PriceBroadcastProcessor(false);
        }
        
        /**
         * 创建要求有效价格的价格广播处理器
         */
        public static PriceBroadcastProcessor createWithRequiredPrices() {
            return new PriceBroadcastProcessor(true);
        }
    }
}
