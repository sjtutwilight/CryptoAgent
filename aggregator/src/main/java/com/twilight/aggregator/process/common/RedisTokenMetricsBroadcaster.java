package com.twilight.aggregator.process.common;

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
 * 使用Redis Broadcast State广播Token指标（价格、市值、FDV、流动性）
 * 替代原来的单独价格广播，提供更完整的Token指标信息
 */
public class RedisTokenMetricsBroadcaster extends BroadcastProcessFunction<ProcessEvent, Map<String, TokenMetrics>, ProcessEvent> {
    private static final Logger log = LoggerFactory.getLogger(RedisTokenMetricsBroadcaster.class);
    
    public static final MapStateDescriptor<String, TokenMetrics> TOKEN_METRICS_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>("token-metrics", String.class, TokenMetrics.class);
    
    
    @Override
    public void processElement(ProcessEvent event, BroadcastProcessFunction<ProcessEvent, Map<String, TokenMetrics>, ProcessEvent>.ReadOnlyContext ctx, Collector<ProcessEvent> out) throws Exception {
        org.apache.flink.api.common.state.ReadOnlyBroadcastState<String, TokenMetrics> metricsState = ctx.getBroadcastState(TOKEN_METRICS_STATE_DESCRIPTOR);
        
        try {
            // 根据事件类型填充token指标信息
            if ("erc20".equals(event.getContractType()) && event.getTokenMetadata() != null) {
                log.debug("📊 Processing ERC20 event, filling token metrics for: {}", event.getTokenMetadata().getAddress());
                String tokenAddress = event.getTokenMetadata().getAddress().toLowerCase();
                TokenMetrics metrics = metricsState.get(tokenAddress);
                if (metrics != null) {
                    event.getTokenMetadata().setTokenMetrics(metrics);
                    log.debug("✅ Set token metrics for {}: price=${}", tokenAddress, metrics.getTokenPriceUsd());
                } else {
                    log.debug("⚠️ No metrics found for token: {}", tokenAddress);
                }
            }
            else if ("dex".equals(event.getContractType()) && event.getPairMetadata() != null) {
                log.debug("📊 Processing DEX event, filling pair token metrics");
                
                // 填充token0指标
                if (event.getPairMetadata().getToken0() != null && event.getPairMetadata().getToken0().getAddress() != null) {
                    String token0Address = event.getPairMetadata().getToken0().getAddress().toLowerCase();
                    TokenMetrics token0Metrics = metricsState.get(token0Address);
                    if (token0Metrics != null) {
                        event.getPairMetadata().getToken0().setTokenMetrics(token0Metrics);
                        log.debug("✅ Set token0 metrics for {}: price=${}", token0Address, token0Metrics.getTokenPriceUsd());
                    } else {
                        log.debug("⚠️ No metrics found for token0: {}", token0Address);
                    }
                }
                
                // 填充token1指标
                if (event.getPairMetadata().getToken1() != null && event.getPairMetadata().getToken1().getAddress() != null) {
                    String token1Address = event.getPairMetadata().getToken1().getAddress().toLowerCase();
                    TokenMetrics token1Metrics = metricsState.get(token1Address);
                    if (token1Metrics != null) {
                        event.getPairMetadata().getToken1().setTokenMetrics(token1Metrics);
                        log.debug("✅ Set token1 metrics for {}: price=${}", token1Address, token1Metrics.getTokenPriceUsd());
                    } else {
                        log.debug("⚠️ No metrics found for token1: {}", token1Address);
                    }
                }
            }
            else {
                log.debug("⚠️ Event has unsupported contract type or missing metadata: contractType={}, hasTokenMetadata={}, hasPairMetadata={}", 
                         event.getContractType(), 
                         event.getTokenMetadata() != null, 
                         event.getPairMetadata() != null);
            }
            
        } catch (Exception e) {
            log.error("💥 Error processing token metrics for event {}: {}", event.getContractAddress(), e.getMessage(), e);
        }
        
        // 输出事件（无论是否成功填充指标）
        out.collect(event);
    }
    
    @Override
    public void processBroadcastElement(Map<String, TokenMetrics> metricsUpdate, BroadcastProcessFunction<ProcessEvent, Map<String, TokenMetrics>, ProcessEvent>.Context ctx, Collector<ProcessEvent> out) throws Exception {
        BroadcastState<String, TokenMetrics> metricsState = ctx.getBroadcastState(TOKEN_METRICS_STATE_DESCRIPTOR);
        
        int updatedCount = 0;
        int priceOnlyCount = 0;
        int fullMetricsCount = 0;
        
        for (Map.Entry<String, TokenMetrics> entry : metricsUpdate.entrySet()) {
            String tokenAddress = entry.getKey().toLowerCase();
            TokenMetrics metrics = entry.getValue();
            
            metricsState.put(tokenAddress, metrics);
            updatedCount++;
            
            if (metrics.hasAllMetrics()) {
                fullMetricsCount++;
                log.trace("📊 Updated full metrics: {} - price=${}, mcap=${}, fdv=${}, liquidity=${}", 
                         tokenAddress, metrics.getTokenPriceUsd(), metrics.getMcap(), 
                         metrics.getFdv(), metrics.getLiquidityUsd());
            } else if (metrics.hasPrice()) {
                priceOnlyCount++;
                log.trace("💰 Updated price only: {} = ${}", tokenAddress, metrics.getTokenPriceUsd());
            }
        }
        
        log.info("🔄 Updated {} token metrics in broadcast state (full metrics: {}, price only: {})", 
                updatedCount, fullMetricsCount, priceOnlyCount);
    }
    
}
