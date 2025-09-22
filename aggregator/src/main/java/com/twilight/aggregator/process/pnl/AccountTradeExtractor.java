package com.twilight.aggregator.process.pnl;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.AccountTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.twilight.aggregator.model.PairMetadata;
import com.twilight.aggregator.model.TokenMetadata;
import java.math.BigDecimal;

/**
 * 账户交易提取器：从ProcessEvent中提取AccountTrade
 * 专门处理Swap事件，提取账户维度的买入/卖出交易信息
 * 依赖前置标准化：UnifiedFilterOperator -> AsyncEventEnrichmentProcessor -> RedisTokenMetricsBroadcaster
 */
public class AccountTradeExtractor extends RichFlatMapFunction<ProcessEvent, AccountTrade> {
    private static final Logger log = LoggerFactory.getLogger(AccountTradeExtractor.class);

    private transient long processedEvents = 0;
    private transient long swapEvents = 0;
    private transient long extractedTrades = 0;
    private transient long skippedEvents = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("🔧 AccountTradeExtractor initialized for PnL processing (no Redis dependency)");
    }

    @Override
    public void flatMap(ProcessEvent event, Collector<AccountTrade> out) throws Exception {
        processedEvents++;


        swapEvents++;
        extractTradesFromSwapData(event, out);

        // 每100个Swap事件记录一次统计
        if (swapEvents % 100 == 0) {
            log.info("📊 Account trade extraction stats - Processed: {}, Swaps: {}, Trades: {}, Skipped: {}",
                    processedEvents, swapEvents, extractedTrades, skippedEvents);
        }
    }

    /**
     * 从强类型Swap数据中提取交易
     */
    private void extractTradesFromSwapData(ProcessEvent event, Collector<AccountTrade> out) {
        // 处理token0交易
            AccountTrade token0Trade = extractTokenTradeFromSwapData(event, true); // true表示token0
            if (token0Trade != null) {
                out.collect(token0Trade);
                extractedTrades++;
                log.trace("💰 Extracted token0 trade: account={}, token={}, side={}, qty={}",
                        token0Trade.getAccountAddress(), token0Trade.getTokenAddress(),
                        token0Trade.getSideString(), token0Trade.getQuantity());
            }
        

        // 处理token1交易
            AccountTrade token1Trade = extractTokenTradeFromSwapData(event, false); // false表示token1
            if (token1Trade != null) {
                out.collect(token1Trade);
                extractedTrades++;
                log.trace("💰 Extracted token1 trade: account={}, token={}, side={}, qty={}",
                        token1Trade.getAccountAddress(), token1Trade.getTokenAddress(),
                        token1Trade.getSideString(), token1Trade.getQuantity());
            }
    }

    /**
     * 从强类型Swap数据中提取单个Token的交易
     */
    private AccountTrade extractTokenTradeFromSwapData(ProcessEvent event, boolean isToken0) {
        try {
            BigDecimal amountIn, amountOut;
            String tokenAddress;
            Long tokenId;
            double tokenPriceUsd;
            ProcessEvent.DexSwapData swapData = event.getDexSwapData();
            PairMetadata pairMetadata = event.getPairMetadata();
            if (isToken0) {
                TokenMetadata tokenMetadata = pairMetadata.getToken0();
                amountIn = swapData.getAmount0In() != null ? swapData.getAmount0In() : BigDecimal.ZERO;
                amountOut = swapData.getAmount0Out() != null ? swapData.getAmount0Out() : BigDecimal.ZERO;
                tokenAddress = tokenMetadata.getAddress();
                tokenId = tokenMetadata.getId();
                tokenPriceUsd = tokenMetadata.getTokenMetrics().getTokenPriceUsd();
            } else {
                TokenMetadata tokenMetadata = pairMetadata.getToken1();
                amountIn = swapData.getAmount1In() != null ? swapData.getAmount1In() : BigDecimal.ZERO;
                amountOut = swapData.getAmount1Out() != null ? swapData.getAmount1Out() : BigDecimal.ZERO;
                tokenAddress = tokenMetadata.getAddress();
                tokenId = tokenMetadata.getId();
                tokenPriceUsd = tokenMetadata.getTokenMetrics().getTokenPriceUsd();
            }

            // 确定交易方向和数量
            AccountTrade.Side side;
            BigDecimal quantity;

            if (amountOut.compareTo(BigDecimal.ZERO) > 0) {
                // token被换出，卖出
                side = AccountTrade.Side.SELL;
                quantity = amountOut;
            } else if (amountIn.compareTo(BigDecimal.ZERO) > 0) {
                // token被换入，买入
                side = AccountTrade.Side.BUY;
                quantity = amountIn;
            } else {
                return null; // 无有效交易
            }

            // 获取账户ID（由上游异步增强提供，缺失时使用默认值）
            Long accountId = event.getAccountMetadata().getId() != null ? event.getAccountMetadata().getId() : 1L;

            AccountTrade trade = new AccountTrade();
            trade.setAccountId(accountId);
            trade.setAccountAddress(event.getFromAddress());
            trade.setTokenId(tokenId);
            trade.setTokenAddress(tokenAddress);
            trade.setSide(side);
            trade.setQuantity(quantity);
            trade.setPriceUsd(BigDecimal.valueOf(tokenPriceUsd));
            trade.setValueUsd(quantity.multiply(BigDecimal.valueOf(tokenPriceUsd)));
            trade.setBlockId(event.getBlockId() != null ? event.getBlockId() : 0L);
            trade.setBlockTimeMs(event.getTimestamp());
            trade.setTxHash(event.getTransactionHash() != null ? event.getTransactionHash() : "");

            return trade;

        } catch (Exception e) {
            log.error("💥 Error extracting trade from swap data: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        log.info("🛑 AccountTradeExtractor closed. Final stats - Processed: {}, Swaps: {}, Trades: {}, Skipped: {}",
                processedEvents, swapEvents, extractedTrades, skippedEvents);
    }
}
