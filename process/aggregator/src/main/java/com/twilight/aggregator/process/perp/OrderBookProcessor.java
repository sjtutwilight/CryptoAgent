package com.twilight.aggregator.process.perp;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.perp.OrderBookData;
import com.twilight.aggregator.model.perp.OrderBookData.PriceLevel;
import com.twilight.aggregator.model.perp.OrderBookSummary;

/**
 * 订单簿处理器（秒级窗口聚合）
 * 
 * 功能：
 * 1. 在窗口内选取最新的订单簿快照（基于seq）
 * 2. 计算订单簿指标：mid_price, spread, depth, imbalance, impact
 * 3. 维护Top-N档位（避免全簿内存爆炸）
 * 
 * 注意：datainjector已经维护完整订单簿（经过integrity+orderbook_diff），
 * 这里直接消费已对齐的L2数据，只需选取窗口内最新快照即可
 */
public class OrderBookProcessor 
        extends ProcessWindowFunction<OrderBookData, OrderBookSummary, String, TimeWindow> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(OrderBookProcessor.class);
    
    // 维护Top-N档位（控制内存）
    private static final int MAX_DEPTH_LEVELS = 200;
    
    // Depth计算阈值（USD）
    private static final BigDecimal DEPTH_10K = BigDecimal.valueOf(10000);
    private static final BigDecimal DEPTH_50K = BigDecimal.valueOf(50000);
    private static final BigDecimal DEPTH_100K = BigDecimal.valueOf(100000);
    
    // Impact计算阈值（USD）
    private static final BigDecimal IMPACT_10K = BigDecimal.valueOf(10000);
    private static final BigDecimal IMPACT_50K = BigDecimal.valueOf(50000);
    private static final BigDecimal IMPACT_100K = BigDecimal.valueOf(100000);
    
    // L1状态：用于计算OFI
    private transient ValueState<L1State> l1StateDescriptor;
    
    /**
     * L1状态：记录上一个窗口的L1价格和数量
     */
    private static class L1State {
        BigDecimal prevBidPrice;
        BigDecimal prevBidSize;
        BigDecimal prevAskPrice;
        BigDecimal prevAskSize;
        
        L1State(BigDecimal prevBidPrice, BigDecimal prevBidSize, 
                BigDecimal prevAskPrice, BigDecimal prevAskSize) {
            this.prevBidPrice = prevBidPrice;
            this.prevBidSize = prevBidSize;
            this.prevAskPrice = prevAskPrice;
            this.prevAskSize = prevAskSize;
        }
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化L1状态（2小时TTL）
        ValueStateDescriptor<L1State> descriptor = new ValueStateDescriptor<>(
            "l1-state",
            L1State.class
        );
        descriptor.enableTimeToLive(org.apache.flink.api.common.state.StateTtlConfig
            .newBuilder(Time.hours(2))
            .setUpdateType(org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        );
        l1StateDescriptor = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public void process(String key, Context context, 
                       Iterable<OrderBookData> elements,
                       Collector<OrderBookSummary> out) throws Exception {
        
        // 1. 选取窗口内最新的订单簿快照（基于seq）
        OrderBookData latestBook = null;
        long maxSeq = -1;
        
        for (OrderBookData book : elements) {
            if (book.getSeq() != null && book.getSeq() > maxSeq) {
                maxSeq = book.getSeq();
                latestBook = book;
            }
        }
        
        if (latestBook == null || latestBook.getDepth() == null) {
            log.warn("No valid orderbook in window for symbol: {}", key);
            return;
        }
        
        List<PriceLevel> bids = latestBook.getDepth().getBids();
        List<PriceLevel> asks = latestBook.getDepth().getAsks();
        
        if (bids == null || bids.isEmpty() || asks == null || asks.isEmpty()) {
            log.warn("Empty bids or asks for symbol: {}", key);
            return;
        }
        
        // 限制档位数量
        bids = bids.subList(0, Math.min(bids.size(), MAX_DEPTH_LEVELS));
        asks = asks.subList(0, Math.min(asks.size(), MAX_DEPTH_LEVELS));
        
        // 2. 计算基础指标
        PriceLevel bestBid = bids.get(0);
        PriceLevel bestAsk = asks.get(0);
        
        BigDecimal midPrice = bestBid.getPrice().add(bestAsk.getPrice())
                .divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_UP);
        
        BigDecimal spreadAbs = bestAsk.getPrice().subtract(bestBid.getPrice());
        BigDecimal spreadBps = spreadAbs.divide(midPrice, 8, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(10000));
        
        // 3. 计算深度指标
        BigDecimal depth10k = calculateDepth(bids, asks, midPrice, DEPTH_10K);
        BigDecimal depth50k = calculateDepth(bids, asks, midPrice, DEPTH_50K);
        BigDecimal depth100k = calculateDepth(bids, asks, midPrice, DEPTH_100K);
        
        // 4. 计算订单簿不平衡
        Double imbalanceTop5 = calculateImbalance(bids, asks, 5);
        Double imbalanceTotal = calculateImbalance(bids, asks, MAX_DEPTH_LEVELS);
        
        // 5. 计算冲击成本
        Double impact10kBps = calculateImpact(asks, midPrice, IMPACT_10K);
        Double impact50kBps = calculateImpact(asks, midPrice, IMPACT_50K);
        Double impact100kBps = calculateImpact(asks, midPrice, IMPACT_100K);
        
        // 6. 构建输出
        OrderBookSummary summary = OrderBookSummary.builder()
                .symbol(latestBook.getSymbol())
                .exchange(latestBook.getExchange())
                .windowEnd(context.window().getEnd())
                .midPrice(midPrice)
                .spreadBps(spreadBps)
                .spreadAbs(spreadAbs)
                .bestBid(bestBid.getPrice())
                .bestAsk(bestAsk.getPrice())
                .bestBidSize(bestBid.getSize())
                .bestAskSize(bestAsk.getSize())
                .depth10k(depth10k)
                .depth50k(depth50k)
                .depth100k(depth100k)
                .imbalanceTop5(imbalanceTop5)
                .imbalanceTotal(imbalanceTotal)
                .impact10kBps(impact10kBps)
                .impact50kBps(impact50kBps)
                .impact100kBps(impact100kBps)
                .latestSeq(maxSeq)
                .processTime(System.currentTimeMillis())
                .build();
        
        // 7. 更新L1状态（供OFI计算使用）
        l1StateDescriptor.update(new L1State(
            bestBid.getPrice(), bestBid.getSize(),
            bestAsk.getPrice(), bestAsk.getSize()
        ));
        
        out.collect(summary);
        
        if (log.isDebugEnabled()) {
            log.debug("OrderBookSummary: symbol={}, mid={}, spread={} bps, depth50k={}", 
                     summary.getSymbol(), summary.getMidPrice(), summary.getSpreadBps(), summary.getDepth50k());
        }
    }
    
    /**
     * 计算深度：在mid_price附近累加到target USD的档位总量
     * 
     * @param bids 买单列表
     * @param asks 卖单列表
     * @param midPrice 中间价
     * @param target 目标USD金额
     * @return 深度（USD）
     */
    private BigDecimal calculateDepth(List<PriceLevel> bids, List<PriceLevel> asks, 
                                      BigDecimal midPrice, BigDecimal target) {
        BigDecimal halfTarget = target.divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_UP);
        BigDecimal bidDepth = BigDecimal.ZERO;
        BigDecimal askDepth = BigDecimal.ZERO;
        
        // 买单深度
        for (PriceLevel level : bids) {
            BigDecimal value = level.getValueUsd();
            bidDepth = bidDepth.add(value);
            if (bidDepth.compareTo(halfTarget) >= 0) {
                break;
            }
        }
        
        // 卖单深度
        for (PriceLevel level : asks) {
            BigDecimal value = level.getValueUsd();
            askDepth = askDepth.add(value);
            if (askDepth.compareTo(halfTarget) >= 0) {
                break;
            }
        }
        
        return bidDepth.add(askDepth);
    }
    
    /**
     * 计算订单簿不平衡
     * imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
     * 
     * @param bids 买单列表
     * @param asks 卖单列表
     * @param topN 前N档
     * @return 不平衡度 (-1 到 1)
     */
    private Double calculateImbalance(List<PriceLevel> bids, List<PriceLevel> asks, int topN) {
        BigDecimal bidVol = BigDecimal.ZERO;
        BigDecimal askVol = BigDecimal.ZERO;
        
        // 买单量
        for (int i = 0; i < Math.min(topN, bids.size()); i++) {
            bidVol = bidVol.add(bids.get(i).getSize());
        }
        
        // 卖单量
        for (int i = 0; i < Math.min(topN, asks.size()); i++) {
            askVol = askVol.add(asks.get(i).getSize());
        }
        
        BigDecimal total = bidVol.add(askVol);
        if (total.compareTo(BigDecimal.ZERO) == 0) {
            return 0.0;
        }
        
        return bidVol.subtract(askVol).divide(total, 8, RoundingMode.HALF_UP).doubleValue();
    }
    
    /**
     * 计算冲击成本：模拟市价买单吃单到target USD的VWAP偏离
     * impact_bps = (vwap - mid_price) / mid_price * 10000
     * 
     * @param asks 卖单列表
     * @param midPrice 中间价
     * @param target 目标USD金额
     * @return 冲击成本（基点）
     */
    private Double calculateImpact(List<PriceLevel> asks, BigDecimal midPrice, BigDecimal target) {
        BigDecimal totalCost = BigDecimal.ZERO;
        BigDecimal totalSize = BigDecimal.ZERO;
        BigDecimal remaining = target;
        
        for (PriceLevel level : asks) {
            BigDecimal price = level.getPrice();
            BigDecimal size = level.getSize();
            BigDecimal value = price.multiply(size);
            
            if (remaining.compareTo(value) >= 0) {
                // 全部吃单
                totalCost = totalCost.add(value);
                totalSize = totalSize.add(size);
                remaining = remaining.subtract(value);
            } else {
                // 部分吃单
                BigDecimal partialSize = remaining.divide(price, 8, RoundingMode.HALF_UP);
                totalCost = totalCost.add(remaining);
                totalSize = totalSize.add(partialSize);
                break;
            }
            
            if (remaining.compareTo(BigDecimal.ZERO) <= 0) {
                break;
            }
        }
        
        if (totalSize.compareTo(BigDecimal.ZERO) == 0) {
            return 0.0;
        }
        
        BigDecimal vwap = totalCost.divide(totalSize, 8, RoundingMode.HALF_UP);
        BigDecimal impactAbs = vwap.subtract(midPrice);
        return impactAbs.divide(midPrice, 8, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(10000)).doubleValue();
    }
}

