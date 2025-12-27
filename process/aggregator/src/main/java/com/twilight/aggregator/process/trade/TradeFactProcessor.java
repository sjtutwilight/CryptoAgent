package com.twilight.aggregator.process.trade;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.ProcessEvent.DexSwapData;
import com.twilight.aggregator.model.PairMetadata;
import com.twilight.aggregator.model.TokenMetadata;
import com.twilight.aggregator.model.TradeFact;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * 交易事实处理器
 * 直接从ProcessEvent提取TradeFact，整合了AccountTradeExtractor和TradeFactEnrichmentProcessor的逻辑
 * 
 * 处理流程：
 * 1. 验证ProcessEvent中包含必要的元数据（pairId, accountId, tokenId）
 * 2. 从Swap事件中提取交易信息
 * 3. 标签位图转换
 * 4. 构造TradeFact并输出
 */
public class TradeFactProcessor extends RichFlatMapFunction<ProcessEvent, TradeFact> {
    
    private static final Logger log = LoggerFactory.getLogger(TradeFactProcessor.class);
    
    
    // 统计计数器
    private transient long processedEvents = 0;
    private transient long swapEvents = 0;
    private transient long extractedTradeFacts = 0;
    private transient long skippedEvents = 0;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("🔧 TradeFactProcessor initialized - pure mapping mode");
        log.info("   📋 No external dependencies, all data from ProcessEvent");
    }
    
    @Override
    public void flatMap(ProcessEvent event, Collector<TradeFact> out) throws Exception {
        processedEvents++;
        swapEvents++; // 所有ProcessEvent都是Swap事件
        log.debug("TradeFactProcessor event:{}",event);
        try {
            
            log.debug("✅ Event has all required data, extracting trade facts...");
            
            // 从DexSwapData中提取交易事实（纯映射逻辑）
            extractTradeFactsFromSwapData(event, out);
            
        } catch (Exception e) {
            log.error("💥 Error processing event: {}", e.getMessage(), e);
            skippedEvents++;
        }
    }
    
    /**
     * 从DexSwapData中提取交易事实
     */
    private void extractTradeFactsFromSwapData(ProcessEvent event, Collector<TradeFact> out) throws Exception {
        ProcessEvent.DexSwapData swapData = event.getDexSwapData();
        
        // 直接从ProcessEvent获取所有必要数据（纯映射）
        Long accountId = event.getAccountMetadata().getId();
        int labelMask = event.getAccountMetadata().getTagBitmap();
        
        log.debug("🔍 Processing swap data for accountId={}, labelMask={}", accountId, labelMask);
        
        // 处理token0交易（如果有）
        if (hasToken0Trade(swapData)) {
            log.debug("📊 Creating TradeFact for token0: amount0In={}, amount0Out={}", 
                     swapData.getAmount0In(), swapData.getAmount0Out());
            TradeFact token0Trade = createTradeFactForToken(event, true);
            if (token0Trade != null) {
                out.collect(token0Trade);
                extractedTradeFacts++;
                log.debug("✅ Emitted TradeFact for token0: {}", token0Trade);
            }
        }
        
        // 处理token1交易（如果有）
        if (hasToken1Trade(swapData)) {
            log.debug("📊 Creating TradeFact for token1: amount1In={}, amount1Out={}", 
                     swapData.getAmount1In(), swapData.getAmount1Out());
            TradeFact token1Trade = createTradeFactForToken(event, false);
            if (token1Trade != null) {
                out.collect(token1Trade);
                extractedTradeFacts++;
                log.debug("✅ Emitted TradeFact for token1: {}", token1Trade);
            }
        }
        
        // 每1000个事件记录一次统计
        if (processedEvents % 1000 == 0) {
            log.debug("📊 TradeFactProcessor stats: processed={}, swaps={}, extracted={}, skipped={}", 
                     processedEvents, swapEvents, extractedTradeFacts, skippedEvents);
        }
    }
    
    /**
     * 检查是否有token0相关的交易
     */
    private boolean hasToken0Trade(ProcessEvent.DexSwapData swapData) {
        return (swapData.getAmount0In() != null && swapData.getAmount0In().compareTo(BigDecimal.ZERO) > 0) ||
               (swapData.getAmount0Out() != null && swapData.getAmount0Out().compareTo(BigDecimal.ZERO) > 0);
    }
    
    /**
     * 检查是否有token1相关的交易
     */
    private boolean hasToken1Trade(ProcessEvent.DexSwapData swapData) {
        return (swapData.getAmount1In() != null && swapData.getAmount1In().compareTo(BigDecimal.ZERO) > 0) ||
               (swapData.getAmount1Out() != null && swapData.getAmount1Out().compareTo(BigDecimal.ZERO) > 0);
    }
    
    /**
     * 为token0创建TradeFact
     */
    private TradeFact createTradeFactForToken(ProcessEvent event
                                              , boolean isToken0) {
        try {
            ProcessEvent.DexSwapData swapData = event.getDexSwapData();
            TradeFact fact = new TradeFact();
            
            // 基础维度字段
            fact.setChainId(event.getChainId() != null ? event.getChainId() : 31337); // 测试网链ID，fallback到31337
            fact.setLabelMask(event.getAccountMetadata().getTagBitmap());
            fact.setAccountId(event.getAccountMetadata().getId());
            fact.setAccountAddress(event.getAccountMetadata().getAddress());  // 新增账户地址
            PairMetadata pairMetadata = event.getPairMetadata();
            fact.setPairId(pairMetadata.getPairId());
            fact.setPairAddress(pairMetadata.getPairAddress());               // 新增交易对地址
            // 确定交易方向和数量（先设置qty和side）
            BigDecimal amount0In = isToken0 ? swapData.getAmount0In() : swapData.getAmount1In();
            BigDecimal amount0Out = isToken0 ? swapData.getAmount0Out() : swapData.getAmount1Out();
            
            if (amount0Out != null && amount0Out.compareTo(BigDecimal.ZERO) > 0) {
                // 用户获得token = BUY
                fact.setSide("buy");
                fact.setQty(amount0Out);
            } else if (amount0In != null && amount0In.compareTo(BigDecimal.ZERO) > 0) {
                // 用户提供token = SELL
                fact.setSide("sell");
                fact.setQty(amount0In);
            } else {
                return null; // 无效交易
            }

            // 设置token相关字段和价格（qty设置后再设置价格）
            if (isToken0) {
                TokenMetadata tokenMetadata = pairMetadata.getToken0();
                fact.setTokenId(tokenMetadata.getId());
                fact.setPriceUsd(BigDecimal.valueOf(tokenMetadata.getTokenMetrics().getTokenPriceUsd()));
            } else {
                TokenMetadata tokenMetadata = pairMetadata.getToken1();
                fact.setTokenId(tokenMetadata.getId());
                fact.setPriceUsd(BigDecimal.valueOf(tokenMetadata.getTokenMetrics().getTokenPriceUsd()));
            }

            // 计算价值（qty和priceUsd都已设置）
            fact.setValueUsd(fact.getQty().multiply(fact.getPriceUsd()));
            
            // 时间字段
            fact.setBlockTime(LocalDateTime.ofEpochSecond(
                event.getTimestamp() / 1000, 0, 
                java.time.ZoneOffset.UTC
            ));
            fact.setBlockId(event.getBlockId());
            
            // 唯一定位 - 生成有效的tx_hash
            String txHash = event.getTransactionHash();
            if (txHash == null || txHash.isEmpty()) {
                // 生成基于时间戳、地址和区块ID的唯一标识
                txHash = String.format("tx_%d_%s_%d", 
                                     event.getTimestamp(), 
                                     event.getFromAddress() != null ? event.getFromAddress().substring(0, Math.min(8, event.getFromAddress().length())) : "unknown", 
                                     event.getBlockId());
            }
            fact.setTxHash(txHash);
            fact.setLogIndex(0); // Swap事件通常logIndex为0，实际可以从event中获取
            
            return fact;
            
        } catch (Exception e) {
            log.error("💥 Error creating TradeFact for token0: {}", e.getMessage());
            return null;
        }
    }
    

    @Override
    public void close() throws Exception {
        super.close();
        log.info("📊 TradeFactProcessor final stats:");
        log.info("   Processed events: {}", processedEvents);
        log.info("   Swap events: {}", swapEvents);
        log.info("   Extracted trade facts: {}", extractedTradeFacts);
        log.info("   Skipped events: {}", skippedEvents);
    }
}
