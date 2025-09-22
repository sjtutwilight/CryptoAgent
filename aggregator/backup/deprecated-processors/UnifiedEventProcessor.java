package com.twilight.aggregator.process;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.config.RedisAsyncConfig;
import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.dexTransaction.Event;
import com.twilight.aggregator.model.dexTransaction.KafkaMessage;
import com.twilight.aggregator.model.PairMetadata;
import com.twilight.aggregator.model.Account;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.api.async.RedisAsyncCommands;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 统一的事件处理器：合并EventExtractor和EventEnrichment
 * 使用Redis查询替代broadcast state，添加详细观测日志
 */
public class UnifiedEventProcessor extends RichFlatMapFunction<KafkaMessage, ProcessEvent> {
    private static final Logger log = LoggerFactory.getLogger(UnifiedEventProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private transient RedisAsyncCommands<String, String> redisAsync;
    private transient Map<String, PairMetadata> pairMetadataCache;
    private transient Map<String, String> accountTagCache; // address -> tag映射
    private transient long lastCacheRefresh;
    private static final long CACHE_REFRESH_INTERVAL = 60000; // 1分钟刷新缓存
    
    // 统计计数器
    private transient long processedMessages = 0;
    private transient long extractedEvents = 0;
    private transient long enrichedEvents = 0;
    private transient long cacheHits = 0;
    private transient long cacheMisses = 0;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RedisAsyncConfig redisConfig = RedisAsyncConfig.getInstance();
        this.redisAsync = redisConfig.getAsyncCommands();
        this.pairMetadataCache = new ConcurrentHashMap<>();
        this.accountTagCache = new ConcurrentHashMap<>();
        this.lastCacheRefresh = 0;
        
        log.info("🚀 UnifiedEventProcessor initialized with Redis connection");
        
        // 立即加载account数据
        refreshAccountTagCache();
        
        // 立即触发一次异步缓存刷新
        this.lastCacheRefresh = 0; // 确保立即刷新
    }
    
    @Override
    public void flatMap(KafkaMessage message, Collector<ProcessEvent> out) throws Exception {
        long startTime = System.currentTimeMillis();
        processedMessages++;
        
        if (message.getEvents() == null || message.getTransaction() == null) {
            log.warn("⚠️ Skipping message with null events or transaction");
            return;
        }
        
        // 定期刷新缓存
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastCacheRefresh > CACHE_REFRESH_INTERVAL) {
            log.info("🔄 Refreshing caches, last refresh was {}ms ago", 
                    currentTime - lastCacheRefresh);
            refreshPairMetadataCache();
            refreshAccountTagCache();
            lastCacheRefresh = currentTime;
        }
        
        String fromAddress = message.getTransaction().getFromAddress();
        Long timestamp = message.getTransaction().getTimestamp();
        
        int eventsInMessage = 0;
        int enrichedInMessage = 0;
        
        for (Event event : message.getEvents()) {
            // 跳过Approval事件
            if ("Approval".equals(event.getEventName())) {
                log.trace("⏭️ Skipping Approval event");
                continue;
            }
            
            ProcessEvent processEvent = createProcessEvent(event, fromAddress, timestamp);
            extractedEvents++;
            eventsInMessage++;
            
            // 丰富事件数据
            boolean enriched = enrichEventWithMetadata(processEvent);
            if (enriched) {
                enrichedEvents++;
                enrichedInMessage++;
            }
            
            // 验证必需字段
            if (isValidProcessEvent(processEvent)) {
                out.collect(processEvent);
                log.trace("✅ Collected process event: {} for contract {}", 
                         event.getEventName(), event.getContractAddress());
            } else {
                log.warn("❌ Invalid process event, skipping: eventName={}, contractAddress={}", 
                        processEvent.getEventName(), processEvent.getContractAddress());
            }
        }
        
        long processingTime = System.currentTimeMillis() - startTime;
        
        // 每100条消息记录一次统计
        if (processedMessages % 100 == 0) {
            log.info("📊 Processing Stats - Messages: {}, Events: {}, Enriched: {}, Cache Hits: {}, Cache Misses: {}", 
                    processedMessages, extractedEvents, enrichedEvents, cacheHits, cacheMisses);
        }
        

    }
    
    private ProcessEvent createProcessEvent(Event event, String fromAddress, Long timestamp) {
        ProcessEvent processEvent = new ProcessEvent();
        processEvent.setEventName(event.getEventName());
        processEvent.setContractAddress(event.getContractAddress().toLowerCase());
        processEvent.setFromAddress(fromAddress);
        processEvent.setTimestamp(timestamp);
        
        // 设置addressTag - 根据fromAddress查找对应的tag
        String tag = "normal"; // 默认tag
        if (fromAddress != null && accountTagCache != null) {
            String foundTag = accountTagCache.get(fromAddress.toLowerCase());
            if (foundTag != null) {
                tag = foundTag;
                log.trace("🏷️ Found tag '{}' for address {}", tag, fromAddress);
            }
        }
        processEvent.setAddressTag(tag);
        
        log.trace("🔧 Created ProcessEvent: eventName={}, contractAddress={}, fromAddress={}, tag={}", 
                 event.getEventName(), event.getContractAddress(), fromAddress, tag);
        
        return processEvent;
    }
    
    private boolean enrichEventWithMetadata(ProcessEvent event) {
        if (event.getContractAddress() == null) {
            log.trace("⚠️ Skipping enrichment for event with null contract address");
            return false;
        }
        
        String contractAddress = event.getContractAddress().toLowerCase();
        PairMetadata metadata = pairMetadataCache.get(contractAddress);
        

        
        if (metadata != null) {
            cacheHits++;
            event.setPairId(metadata.getPairId());
            event.setToken0Id(metadata.getToken0Id());
            event.setToken1Id(metadata.getToken1Id());
            event.setToken0Address(metadata.getToken0Address());
            event.setToken1Address(metadata.getToken1Address());
            
            // 设置地址标签
            if (metadata.getAddressTagMap() != null && event.getFromAddress() != null) {
                String tag = metadata.getAddressTagMap().get(event.getFromAddress().toLowerCase());
                if (tag != null) {
                    event.setAddressTag(tag);
                    log.trace("🏷️ Set address tag '{}' for address {}", tag, event.getFromAddress());
                }
            }

            return true;
        } else {
            cacheMisses++;
            log.trace("❓ No metadata found for contract address: {}", contractAddress);
            return false;
        }
    }
    
    private void refreshPairMetadataCache() {
        try {
            log.debug("🔄 Refreshing pair metadata cache");
            long startTime = System.currentTimeMillis();
            CompletableFuture<String> future = redisAsync.get("pairMetadata").toCompletableFuture();
            
            future.thenAccept(result -> {
                if (result != null) {
                    try {
                        log.debug("🔍 Parsing pair metadata from Redis: {}", result);
                        List<PairMetadata> pairMetadataList = objectMapper.readValue(
                            result, new TypeReference<List<PairMetadata>>() {});
                        
                        Map<String, PairMetadata> newCache = new ConcurrentHashMap<>();
                        for (PairMetadata metadata : pairMetadataList) {
                            String pairAddr = metadata.getPairAddress();
                            if (pairAddr != null) {
                                newCache.put(pairAddr.toLowerCase(), metadata);
                                log.trace("📦 Cached pair metadata: address={}, token0={}, token1={}", 
                                         pairAddr, metadata.getToken0Address(), metadata.getToken1Address());
                            }
                        }
                        
                        this.pairMetadataCache = newCache;
                        long refreshTime = System.currentTimeMillis() - startTime;
                        log.info("🎯 Refreshed pair metadata cache with {} entries in {}ms", 
                                newCache.size(), refreshTime);
                        
                    } catch (Exception e) {
                        log.error("💥 Failed to parse pair metadata from Redis", e);
                    }
                } else {
                    log.warn("⚠️ No pair metadata found in Redis");
                }
            }).exceptionally(throwable -> {
                log.error("💥 Failed to fetch pair metadata from Redis", throwable);
                return null;
            });
            
            // 等待最多100ms
            future.get(100, TimeUnit.MILLISECONDS);
            
        } catch (Exception e) {
            log.error("💥 Error refreshing pair metadata cache", e);
        }
    }
    
    private void refreshAccountTagCache() {
        try {
            log.debug("🔄 Refreshing account tag cache");
            CompletableFuture<String> future = redisAsync.get("accountMetadata").toCompletableFuture();
            
            future.thenAccept(result -> {
                if (result != null) {
                    try {
                        log.debug("🔍 Parsing account data from Redis");
                        List<Account> accounts = objectMapper.readValue(
                            result, new TypeReference<List<Account>>() {});
                        
                        Map<String, String> newCache = new ConcurrentHashMap<>();
                        for (Account account : accounts) {
                            if (account.getAddress() != null && account.getTag() != null) {
                                newCache.put(account.getAddress().toLowerCase(), account.getTag());
                                log.info("📦 Cached account: address={}, tag={}", 
                                         account.getAddress(), account.getTag());
                            }
                        }
                        
                        this.accountTagCache = newCache;
                        log.info("🎯 Refreshed account tag cache with {} entries", newCache.size());
                        
                    } catch (Exception e) {
                        log.error("💥 Failed to parse account data from Redis", e);
                    }
                } else {
                    log.warn("⚠️ No account data found in Redis");
                }
            }).exceptionally(throwable -> {
                log.error("💥 Failed to fetch account data from Redis", throwable);
                return null;
            });
            
            // 等待最多100ms
            future.get(100, TimeUnit.MILLISECONDS);
            
        } catch (Exception e) {
            log.error("💥 Error refreshing account tag cache", e);
        }
    }
    
    private boolean isValidProcessEvent(ProcessEvent event) {
        boolean valid = event.getEventName() != null 
            && event.getContractAddress() != null
            && event.getFromAddress() != null
            && event.getTimestamp() != null;
            
        if (!valid) {
            log.trace("❌ Invalid event: eventName={}, contractAddress={}, fromAddress={}, timestamp={}", 
                     event.getEventName(), event.getContractAddress(), 
                    event.getFromAddress(), event.getTimestamp());
        }
        
        return valid;
    }
    
    
    @Override
    public void close() throws Exception {
        super.close();
        log.info("🛑 UnifiedEventProcessor closed. Final stats - Messages: {}, Events: {}, Enriched: {}", 
                processedMessages, extractedEvents, enrichedEvents);
    }
}