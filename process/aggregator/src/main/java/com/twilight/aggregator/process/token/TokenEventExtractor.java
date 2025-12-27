package com.twilight.aggregator.process.token;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.Token;
import com.twilight.aggregator.utils.EthereumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.twilight.aggregator.model.PairMetadata;
import com.twilight.aggregator.model.TokenMetadata;
import com.twilight.aggregator.model.TokenMetrics;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Token事件提取器：专门从ProcessEvent中提取Token相关事件
 * 仅处理Swap事件，添加详细观测日志
 */
public class TokenEventExtractor extends RichFlatMapFunction<ProcessEvent, Token> {
    private static final Logger log = LoggerFactory.getLogger(TokenEventExtractor.class);
    
    private transient long processedEvents = 0;
    private transient long swapEvents = 0;
    private transient long tokenEvents = 0;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("🔧 TokenEventExtractor initialized");
    }
    
    @Override
    public void flatMap(ProcessEvent event, Collector<Token> out) throws Exception {
        processedEvents++;
    
        
        swapEvents++;
        
        processSwapTokens(event, out);
        
        // 每100个Swap事件记录一次统计
        if (swapEvents % 100 == 0) {
            log.info("📊 Token extraction stats - Processed: {}, Swaps: {}, Tokens: {}", 
                    processedEvents, swapEvents, tokenEvents);
        }
    }
    

    /**
     * 使用强类型数据处理Swap中的Token事件
     */
    private void processSwapTokens(ProcessEvent event, Collector<Token> out) {
        ProcessEvent.DexSwapData swapData = event.getDexSwapData();
        PairMetadata pairMetadata = event.getPairMetadata();

        if (swapData == null) {
            return;
        }
        // 处理token0
        if 
            (isPositiveBigDecimal(swapData.getAmount0In()) || isPositiveBigDecimal(swapData.getAmount0Out())) {
            TokenMetadata tokenMetadata = pairMetadata.getToken0();
            Token token0 = createTokenFromSwapData(event, true); // true表示token0
            if (token0 != null) {
                out.collect(token0);
                tokenEvents++;
                log.trace("💰 Created token0 event: address={}, amount={}, buyOrSell={}", 
                         token0.getTokenAddress(), token0.getAmount(), token0.isBuyOrSell());
            }
        }
        
        // 处理token1
        if ( 
            (isPositiveBigDecimal(swapData.getAmount1In()) || isPositiveBigDecimal(swapData.getAmount1Out()))) {
            
            Token token1 = createTokenFromSwapData(event, false); // false表示token1
            if (token1 != null) {
                out.collect(token1);
                tokenEvents++;
                log.trace("💰 Created token1 event: address={}, amount={}, buyOrSell={}", 
                         token1.getTokenAddress(), token1.getAmount(), token1.isBuyOrSell());
            }
        }
    }
    
    /**
     * 从强类型Swap数据创建Token对象
     */
    private Token createTokenFromSwapData(ProcessEvent event, boolean isToken0) {
        try {
            Token token = new Token();
    
            ProcessEvent.DexSwapData swapData = event.getDexSwapData();
            // 设置基础信息
            if (isToken0) {
                TokenMetadata tokenMetadata = event.getPairMetadata().getToken0();

                TokenMetrics tokenMetrics = tokenMetadata.getTokenMetrics();
                token.setTokenId(  tokenMetadata.getId());
                token.setTokenAddress(tokenMetadata.getAddress());
                token.setTokenPriceUsd(tokenMetrics.getTokenPriceUsd());
                token.setFromAddressTag(event.getAccountMetadata().getTag());
                // 设置金额和买卖方向
                BigDecimal amountIn = swapData.getAmount0In() != null ? swapData.getAmount0In() : BigDecimal.ZERO;
                BigDecimal amountOut = swapData.getAmount0Out() != null ? swapData.getAmount0Out() : BigDecimal.ZERO;
                
                if (amountOut.compareTo(BigDecimal.ZERO) > 0) {
                    // token0被换出，表示卖出token0
                    token.setAmount(amountOut.doubleValue());
                    token.setBuyOrSell(false); // 卖出
                } else if (amountIn.compareTo(BigDecimal.ZERO) > 0) {
                    // token0被换入，表示买入token0
                    token.setAmount(amountIn.doubleValue());
                    token.setBuyOrSell(true); // 买入
                }
                token.setMcapUsd(tokenMetrics.getMcap());
                token.setFdvUsd(tokenMetrics.getFdv());
                token.setLiquidityUsd(tokenMetrics.getLiquidityUsd());
            } else {
                TokenMetadata tokenMetadata = event.getPairMetadata().getToken1();
                TokenMetrics tokenMetrics = tokenMetadata.getTokenMetrics();
                token.setTokenId(tokenMetadata.getId());
                token.setTokenAddress(tokenMetadata.getAddress());
                token.setTokenPriceUsd(tokenMetrics.getTokenPriceUsd());
                
                // 设置金额和买卖方向
                BigDecimal amountIn = swapData.getAmount1In() != null ? swapData.getAmount1In() : BigDecimal.ZERO;
                BigDecimal amountOut = swapData.getAmount1Out() != null ? swapData.getAmount1Out() : BigDecimal.ZERO;
                
                if (amountOut.compareTo(BigDecimal.ZERO) > 0) {
                    // token1被换出，表示卖出token1
                    token.setAmount(amountOut.doubleValue());
                    token.setBuyOrSell(false); // 卖出
                } else if (amountIn.compareTo(BigDecimal.ZERO) > 0) {
                    // token1被换入，表示买入token1
                    token.setAmount(amountIn.doubleValue());
                    token.setBuyOrSell(true); // 买入
                }
                token.setMcapUsd(tokenMetrics.getMcap());
                token.setFdvUsd(tokenMetrics.getFdv());
                token.setLiquidityUsd(tokenMetrics.getLiquidityUsd());
                token.setFromAddressTag(event.getAccountMetadata().getTag());
            }
            
            // 设置通用字段
            token.setTimestamp(event.getTimestamp());
            token.setFromAddress(event.getFromAddress());


            return token;
            
        } catch (Exception e) {
            log.error("💥 Error creating token from swap data: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 检查BigDecimal是否为正数
     */
    private boolean isPositiveBigDecimal(BigDecimal amount) {
        return amount != null && amount.compareTo(BigDecimal.ZERO) > 0;
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        log.info("🛑 TokenEventExtractor closed. Final stats - Processed: {}, Swaps: {}, Tokens: {}", 
                processedEvents, swapEvents, tokenEvents);
    }
}
