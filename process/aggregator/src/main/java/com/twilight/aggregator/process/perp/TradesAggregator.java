package com.twilight.aggregator.process.perp;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.perp.TradeData;
import com.twilight.aggregator.model.perp.TradesSummary;

/**
 * 成交数据聚合器（秒级窗口聚合）
 * 
 * 功能：
 * 1. 统计窗口内的成交笔数和成交量
 * 2. 计算VWAP（成交量加权平均价格）
 * 3. 区分主动买入和主动卖出成交量
 * 4. 记录价格范围（high/low/first/last）
 */
public class TradesAggregator 
        extends ProcessWindowFunction<TradeData, TradesSummary, String, TimeWindow> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(TradesAggregator.class);
    
    @Override
    public void process(String key, Context context, 
                       Iterable<TradeData> elements,
                       Collector<TradesSummary> out) throws Exception {
        
        int tradeCount = 0;
        BigDecimal totalValue = BigDecimal.ZERO;  // 总成交额
        BigDecimal totalSize = BigDecimal.ZERO;   // 总成交量
        BigDecimal buyVolume = BigDecimal.ZERO;   // 主动买入成交额
        BigDecimal sellVolume = BigDecimal.ZERO;  // 主动卖出成交额
        
        BigDecimal highPrice = null;
        BigDecimal lowPrice = null;
        BigDecimal firstPrice = null;
        BigDecimal lastPrice = null;
        long firstTs = Long.MAX_VALUE;
        long lastTs = Long.MIN_VALUE;
        
        String symbol = null;
        String exchange = null;
        
        // 遍历窗口内的所有成交
        for (TradeData trade : elements) {
            if (symbol == null) {
                symbol = trade.getSymbol();
                exchange = trade.getExchange();
            }
            
            tradeCount++;
            
            BigDecimal price = trade.getPrice();
            BigDecimal size = trade.getSize();
            BigDecimal value = trade.getValueUsd();
            
            // 累加总量
            totalValue = totalValue.add(value);
            totalSize = totalSize.add(size);
            
            // 区分买卖方向
            if (trade.isBuyerTaker()) {
                // 主动买入
                buyVolume = buyVolume.add(value);
            } else {
                // 主动卖出
                sellVolume = sellVolume.add(value);
            }
            
            // 更新价格范围
            if (highPrice == null || price.compareTo(highPrice) > 0) {
                highPrice = price;
            }
            if (lowPrice == null || price.compareTo(lowPrice) < 0) {
                lowPrice = price;
            }
            
            // 记录首尾价格
            Long tradeTs = trade.getExchangeTs();
            if (tradeTs != null) {
                if (tradeTs < firstTs) {
                    firstTs = tradeTs;
                    firstPrice = price;
                }
                if (tradeTs > lastTs) {
                    lastTs = tradeTs;
                    lastPrice = price;
                }
            }
        }
        
        // 如果窗口内没有成交，输出空summary
        if (tradeCount == 0) {
            log.debug("No trades in window for symbol: {}", key);
            // 仍然输出，但所有指标为0，便于与订单簿数据join
            out.collect(TradesSummary.builder()
                    .symbol(key.split("_")[0]) // 从key中提取symbol
                    .exchange("binance")
                    .windowEnd(context.window().getEnd())
                    .tradeCount(0)
                    .volumeUsd(BigDecimal.ZERO)
                    .vwap(null)
                    .buyVolumeUsd(BigDecimal.ZERO)
                    .sellVolumeUsd(BigDecimal.ZERO)
                    .buyRatio(0.0)
                    .processTime(System.currentTimeMillis())
                    .build());
            return;
        }
        
        // 计算VWAP = 总成交额 / 总成交量
        BigDecimal vwap = null;
        if (totalSize.compareTo(BigDecimal.ZERO) > 0) {
            vwap = totalValue.divide(totalSize, 8, RoundingMode.HALF_UP);
        }
        
        // 计算买卖比例
        Double buyRatio = 0.0;
        BigDecimal totalTradeVol = buyVolume.add(sellVolume);
        if (totalTradeVol.compareTo(BigDecimal.ZERO) > 0) {
            buyRatio = buyVolume.divide(totalTradeVol, 8, RoundingMode.HALF_UP).doubleValue();
        }
        
        // 构建输出
        TradesSummary summary = TradesSummary.builder()
                .symbol(symbol)
                .exchange(exchange)
                .windowEnd(context.window().getEnd())
                .tradeCount(tradeCount)
                .volumeUsd(totalValue)
                .vwap(vwap)
                .buyVolumeUsd(buyVolume)
                .sellVolumeUsd(sellVolume)
                .buyRatio(buyRatio)
                .highPrice(highPrice)
                .lowPrice(lowPrice)
                .firstPrice(firstPrice)
                .lastPrice(lastPrice)
                .processTime(System.currentTimeMillis())
                .build();
        
        out.collect(summary);
        
        if (log.isDebugEnabled()) {
            log.debug("TradesSummary: symbol={}, count={}, volume={}, vwap={}, buyRatio={}", 
                     summary.getSymbol(), summary.getTradeCount(), summary.getVolumeUsd(), 
                     summary.getVwap(), summary.getBuyRatio());
        }
    }
}







