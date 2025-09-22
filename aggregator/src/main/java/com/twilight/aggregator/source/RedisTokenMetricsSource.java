package com.twilight.aggregator.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import com.twilight.aggregator.config.RedisAsyncConfig;
import com.twilight.aggregator.model.TokenMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.api.async.RedisAsyncCommands;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Redis Token指标数据源
 * 定期从Redis获取token的价格、市值、FDV、流动性等指标并广播
 */
public class RedisTokenMetricsSource extends RichSourceFunction<Map<String, TokenMetrics>> {
    private static final Logger log = LoggerFactory.getLogger(RedisTokenMetricsSource.class);
    
    private final long refreshIntervalMs;
    private volatile boolean running = true;
    
    private transient RedisAsyncCommands<String, String> redisAsync;
    
    public RedisTokenMetricsSource(long refreshIntervalMs) {
        this.refreshIntervalMs = refreshIntervalMs;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RedisAsyncConfig redisConfig = RedisAsyncConfig.getInstance();
        this.redisAsync = redisConfig.getAsyncCommands();
        
        log.info("🚀 RedisTokenMetricsSource initialized, refresh interval: {}ms", refreshIntervalMs);
    }
    
    @Override
    public void run(SourceContext<Map<String, TokenMetrics>> ctx) throws Exception {
        while (running) {
            try {
                long startTime = System.currentTimeMillis();
                
                // 获取所有token指标
                Map<String, TokenMetrics> metricsMap = fetchTokenMetrics();
                
                if (metricsMap != null && !metricsMap.isEmpty()) {
                    ctx.collect(metricsMap);
                    
                    long fetchTime = System.currentTimeMillis() - startTime;
                    log.info("📡 Broadcasted {} token metrics, fetch time: {}ms", metricsMap.size(), fetchTime);
                    
                    // 记录部分指标用于调试
                    if (log.isDebugEnabled()) {
                        int count = 0;
                        for (Map.Entry<String, TokenMetrics> entry : metricsMap.entrySet()) {
                            if (count++ < 3) { // 只记录前3个
                                TokenMetrics metrics = entry.getValue();
                                log.debug("📊 Metrics: {} - price=${}, mcap=${}, fdv=${}, liquidity=${}", 
                                    entry.getKey(), metrics.getTokenPriceUsd(), metrics.getMcap(), 
                                    metrics.getFdv(), metrics.getLiquidityUsd());
                            } else {
                                break;
                            }
                        }
                    }
                } else {
                    log.warn("⚠️ No token metrics fetched from Redis");
                }
                
            } catch (Exception e) {
                log.error("💥 Error fetching token metrics from Redis", e);
            }
            
            // 等待下次刷新
            try {
                Thread.sleep(refreshIntervalMs);
            } catch (InterruptedException e) {
                log.info("🛑 RedisTokenMetricsSource interrupted, stopping...");
                running = false;
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private Map<String, TokenMetrics> fetchTokenMetrics() {
        try {
            // 获取所有token地址（从价格keys推断）
            CompletableFuture<java.util.List<String>> priceKeysFuture = redisAsync.keys("token_price:*").toCompletableFuture();
            java.util.List<String> priceKeys = priceKeysFuture.get(5, TimeUnit.SECONDS);
            
            if (priceKeys == null || priceKeys.isEmpty()) {
                log.debug("🔍 No token_price keys found in Redis");
                return new HashMap<>();
            }
            
            // 提取token地址
            Set<String> tokenAddresses = new HashSet<>();
            for (String priceKey : priceKeys) {
                if (priceKey.startsWith("token_price:")) {
                    String address = priceKey.substring("token_price:".length());
                    tokenAddresses.add(address);
                }
            }
            
            log.debug("🔑 Found {} token addresses", tokenAddresses.size());
            
            Map<String, TokenMetrics> metricsMap = new HashMap<>();
            
            // 为每个token获取所有指标
            for (String tokenAddress : tokenAddresses) {
                try {
                    TokenMetrics metrics = fetchSingleTokenMetrics(tokenAddress);
                    if (metrics != null && metrics.hasPrice()) {
                        metricsMap.put(tokenAddress.toLowerCase(), metrics);
                    }
                } catch (Exception e) {
                    log.warn("❌ Error fetching metrics for token {}: {}", tokenAddress, e.getMessage());
                }
            }
            
            log.debug("📊 Fetched metrics for {} tokens", metricsMap.size());
            return metricsMap;
            
        } catch (Exception e) {
            log.error("❌ Failed to fetch token metrics from Redis", e);
            return new HashMap<>();
        }
    }
    
    private TokenMetrics fetchSingleTokenMetrics(String tokenAddress) throws Exception {
        TokenMetrics metrics = new TokenMetrics(tokenAddress);
        
        // 准备所有Redis keys
        String priceKey = "token_price:" + tokenAddress;
        String mcapKey = "token_mcap:" + tokenAddress;
        String fdvKey = "token_fdv:" + tokenAddress;
        String liquidityKey = "token_liquidity:" + tokenAddress;
        
        // 批量获取所有指标
        CompletableFuture<java.util.List<io.lettuce.core.KeyValue<String, String>>> valuesFuture = 
            redisAsync.mget(priceKey, mcapKey, fdvKey, liquidityKey).toCompletableFuture();
        java.util.List<io.lettuce.core.KeyValue<String, String>> keyValues = valuesFuture.get(3, TimeUnit.SECONDS);
        
        if (keyValues == null || keyValues.size() != 4) {
            log.debug("🔍 Incomplete metrics data for token {}", tokenAddress);
            return null;
        }
        
        boolean hasAnyMetric = false;
        
        // 解析各个指标
        for (io.lettuce.core.KeyValue<String, String> kv : keyValues) {
            if (kv != null && kv.hasValue()) {
                String key = kv.getKey();
                String value = kv.getValue();
                
                try {
                    Double numValue = Double.parseDouble(value);
                    
                    if (key.equals(priceKey)) {
                        metrics.setTokenPriceUsd(numValue);
                        hasAnyMetric = true;
                    } else if (key.equals(mcapKey)) {
                        metrics.setMcap(numValue);
                        hasAnyMetric = true;
                    } else if (key.equals(fdvKey)) {
                        metrics.setFdv(numValue);
                        hasAnyMetric = true;
                    } else if (key.equals(liquidityKey)) {
                        metrics.setLiquidityUsd(numValue);
                        hasAnyMetric = true;
                    }
                    
                    log.trace("📊 Metric: {} = {}", key, numValue);
                } catch (NumberFormatException e) {
                    log.warn("❌ Invalid metric format for {}: {}", key, value);
                }
            }
        }
        
        if (!hasAnyMetric) {
            log.debug("❌ No valid metrics found for token {}", tokenAddress);
            return null;
        }
        
        log.trace("✅ Fetched metrics for token {}: price={}, mcap={}, fdv={}, liquidity={}", 
                 tokenAddress, metrics.getTokenPriceUsd(), metrics.getMcap(), 
                 metrics.getFdv(), metrics.getLiquidityUsd());
        
        return metrics;
    }
    
    @Override
    public void cancel() {
        log.info("🛑 Cancelling RedisTokenMetricsSource...");
        running = false;
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        log.info("🛑 RedisTokenMetricsSource closed");
    }
}
