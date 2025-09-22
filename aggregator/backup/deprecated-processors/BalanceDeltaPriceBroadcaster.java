package com.twilight.aggregator.process;

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.util.Collector;

import com.twilight.aggregator.model.BalanceDelta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;

import com.twilight.aggregator.model.TokenMetrics;

/**
 * BalanceDelta价格增强处理器
 * 使用Redis Broadcast State为BalanceDelta添加价格信息
 */
public class BalanceDeltaPriceBroadcaster extends BroadcastProcessFunction<BalanceDelta, Map<String, TokenMetrics>, BalanceDelta> {
    private static final Logger log = LoggerFactory.getLogger(BalanceDeltaPriceBroadcaster.class);
    
    public static final MapStateDescriptor<String, Double> PRICE_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>("token-prices", String.class, Double.class);
    
    // 统计计数器
    private transient long processedDeltas = 0;
    private transient long enrichedDeltas = 0;
    private transient long priceHits = 0;
    private transient long priceMisses = 0;
    
    @Override
    public void processElement(BalanceDelta delta, BroadcastProcessFunction<BalanceDelta, Map<String, TokenMetrics>, BalanceDelta>.ReadOnlyContext ctx, Collector<BalanceDelta> out) throws Exception {
        processedDeltas++;
        org.apache.flink.api.common.state.ReadOnlyBroadcastState<String, Double> priceState = ctx.getBroadcastState(PRICE_STATE_DESCRIPTOR);
        
        boolean enriched = false;
        
        // 获取合约地址的价格信息
        if (delta.getContractAddress() != null) {
            String contractKey = delta.getContractAddress().toLowerCase();
            Double tokenPrice = priceState.get(contractKey);
            
            if (tokenPrice != null) {
                delta.setPriceUsd(BigDecimal.valueOf(tokenPrice));
                priceHits++;
                enriched = true;
                log.trace("💰 Set price for contract {}: ${}", contractKey, tokenPrice);
            } else {
                priceMisses++;
                log.trace("❓ No price found for contract: {}", contractKey);
            }
        }
        
        if (enriched) {
            enrichedDeltas++;
        }
        
        // 输出增强后的BalanceDelta
        out.collect(delta);
        
        // 每1000个事件记录一次统计
        if (processedDeltas % 1000 == 0) {
            log.info("📊 BalanceDelta price enrichment stats - Processed: {}, Enriched: {}, Hits: {}, Misses: {}", 
                    processedDeltas, enrichedDeltas, priceHits, priceMisses);
        }
        
        log.debug("✅ Processed BalanceDelta for account {}, contract {}, enriched: {}", 
                 delta.getAccountAddress(), delta.getContractAddress(), enriched);
    }
    
    @Override
    public void processBroadcastElement(Map<String, TokenMetrics> priceUpdate, BroadcastProcessFunction<BalanceDelta, Map<String, TokenMetrics>, BalanceDelta>.Context ctx, Collector<BalanceDelta> out) throws Exception {
        BroadcastState<String, Double> priceState = ctx.getBroadcastState(PRICE_STATE_DESCRIPTOR);
        
        int updatedCount = 0;
        for (Map.Entry<String, TokenMetrics> entry : priceUpdate.entrySet()) {
            String tokenAddress = entry.getKey().toLowerCase();
            Double price = entry.getValue().getTokenPriceUsd();
            
            priceState.put(tokenAddress, price);
            updatedCount++;
            
            log.trace("💰 Updated price for BalanceDelta processing: {} = ${}", tokenAddress, price);
        }
        
        log.info("🔄 Updated {} token prices in BalanceDelta broadcast state", updatedCount);
    }
}
