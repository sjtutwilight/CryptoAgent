package com.twilight.aggregator.process.perp;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.perp.MarkIndexData;
import com.twilight.aggregator.model.perp.FundingData;
import com.twilight.aggregator.model.perp.OpenInterestData;
import com.twilight.aggregator.model.perp.ContextMetrics;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 语境面分钟快照器（GPT建议的P0修复）
 * 
 * 问题：慢流数据（Mark/Index/Funding/OI）更新频率不同，
 * 如果直接用窗口聚合，可能在某分钟内拿不到最新值。
 * 
 * 解决方案：
 * 1. 使用ValueState维护每个symbol的最新状态
 * 2. 每到分钟边界（通过定时器），按state的最新值生成ContextMetrics
 * 3. 这样即使某字段在该分钟没新事件，也能输出前值（前填补）
 * 
 * 定时器触发：每分钟整点（UTC对齐）
 */
public class ContextSnapshotTimer 
        extends KeyedProcessFunction<String, Object, ContextMetrics> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(ContextSnapshotTimer.class);
    
    // 算法版本
    private static final String ALGO_VERSION = "v1.0";
    
    // 分钟对齐间隔（毫秒）
    private static final long MINUTE_MS = 60 * 1000;
    
    // 状态：维护最新的Mark/Index/Funding/OI
    private transient ValueState<LatestState> latestState;
    
    /**
     * 最新状态：存储每个symbol的最新语境数据
     * 注意：必须是public static，否则Kryo序列化会失败
     */
    public static class LatestState {
        // Mark/Index（public字段让Kryo可以访问）
        public BigDecimal markPrice;
        public BigDecimal indexPrice;
        public Long markIndexTs;
        
        // Funding
        public BigDecimal fundingRate;
        public String fundingInterval;
        public Long nextFundingTime;
        public Long fundingTs;
        
        // Funding EMA（在线计算）
        public BigDecimal fundingEma24h;
        public Long lastEmaUpdateTs;
        
        // OI
        public BigDecimal oi;
        public BigDecimal oiUsd;
        public Long oiTs;
        public Boolean isOiCarried; // 是否为前值填充
        
        // 上一分钟的OI（用于计算delta）
        public BigDecimal prevOiUsd;
        
        public LatestState() {
            this.isOiCarried = false;
        }
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化状态（24小时TTL）
        ValueStateDescriptor<LatestState> descriptor = new ValueStateDescriptor<>(
            "context-latest-state",
            LatestState.class
        );
        descriptor.enableTimeToLive(org.apache.flink.api.common.state.StateTtlConfig
            .newBuilder(Time.hours(24))
            .setUpdateType(org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        );
        latestState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public void processElement(Object value, Context ctx, Collector<ContextMetrics> out) 
            throws Exception {
        
        LatestState state = latestState.value();
        if (state == null) {
            state = new LatestState();
        }
        
        // 根据数据类型更新状态
        if (value instanceof MarkIndexData) {
            MarkIndexData data = (MarkIndexData) value;
            state.markPrice = data.getMarkPrice();
            state.indexPrice = data.getIndexPrice();
            state.markIndexTs = data.getExchangeTs();
            
            // 更新funding rate（从mark/index推送中也包含）
            if (data.getLastFundingRate() != null) {
                state.fundingRate = data.getLastFundingRate();
                state.nextFundingTime = data.getNextFundingTime();
                
                // 更新在线EMA
                updateFundingEMA(state, data.getLastFundingRate(), data.getExchangeTs());
            }
            
        } else if (value instanceof FundingData) {
            FundingData data = (FundingData) value;
            state.fundingRate = data.getFundingRate();
            state.fundingInterval = data.getFundingInterval();
            state.fundingTs = data.getExchangeTs();
            
            // 更新在线EMA
            updateFundingEMA(state, data.getFundingRate(), data.getExchangeTs());
            
        } else if (value instanceof OpenInterestData) {
            OpenInterestData data = (OpenInterestData) value;
            state.oi = data.getOi();
            state.oiUsd = data.getOiUsd();
            state.oiTs = data.getExchangeTs();
            state.isOiCarried = false; // 真实数据
        }
        
        // 更新状态
        latestState.update(state);
        
        // 注册下一个分钟边界的定时器
        long currentTime = ctx.timestamp() != null ? ctx.timestamp() : System.currentTimeMillis();
        long nextMinuteBoundary = ((currentTime / MINUTE_MS) + 1) * MINUTE_MS;
        ctx.timerService().registerEventTimeTimer(nextMinuteBoundary);
        
        if (log.isTraceEnabled()) {
            log.trace("Updated state for symbol={}, nextTimer={}", ctx.getCurrentKey(), nextMinuteBoundary);
        }
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ContextMetrics> out) 
            throws Exception {
        
        LatestState state = latestState.value();
        if (state == null) {
            log.warn("No state for key={} at timer {}", ctx.getCurrentKey(), timestamp);
            return;
        }
        
        // 解析key：symbol@exchange
        String key = ctx.getCurrentKey();
        String[] parts = key.split("@");
        String symbol = parts.length > 0 ? parts[0] : key;
        String exchange = parts.length > 1 ? parts[1] : "unknown";
        
        // 计算基差
        Double basisBps = null;
        if (state.markPrice != null && state.indexPrice != null && 
            state.indexPrice.compareTo(BigDecimal.ZERO) != 0) {
            basisBps = state.markPrice.subtract(state.indexPrice)
                    .divide(state.indexPrice, 8, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(10000)).doubleValue();
        }
        
        // 计算OI变化（使用零值代替null，满足ClickHouse非null约束）
        BigDecimal oiDelta = BigDecimal.ZERO;
        Double oiDeltaPct = 0.0;
        if (state.oiUsd != null && state.prevOiUsd != null) {
            oiDelta = state.oiUsd.subtract(state.prevOiUsd);
            if (state.prevOiUsd.compareTo(BigDecimal.ZERO) != 0) {
                oiDeltaPct = oiDelta.divide(state.prevOiUsd, 8, RoundingMode.HALF_UP)
                        .multiply(BigDecimal.valueOf(100)).doubleValue();
            }
        }
        
        // OI前值填充标记
        Boolean isOiCarried = state.isOiCarried;
        
        // 构建ContextMetrics
        ContextMetrics metrics = ContextMetrics.builder()
                .symbol(symbol)
                .exchange(exchange)  // 从key解析（symbol@exchange）
                .endTime(timestamp)
                .algoVersion(ALGO_VERSION)
                .markPrice(state.markPrice)
                .indexPrice(state.indexPrice)
                .basisBps(basisBps)
                .fundingRate(state.fundingRate)
                .fundingRate8h(state.fundingRate) // Binance默认8h
                .fundingEma24h(state.fundingEma24h)
                .nextFundingTime(state.nextFundingTime)
                .oi(state.oi)
                .oiUsd(state.oiUsd)
                .oiDelta1m(oiDelta)
                .oiDeltaPct(oiDeltaPct)
                .isOiCarried(isOiCarried)
                .processTime(System.currentTimeMillis())
                .build();
        
        out.collect(metrics);
        
        // 更新prevOiUsd用于下一分钟计算delta
        state.prevOiUsd = state.oiUsd;
        
        // 如果OI在这一分钟没更新，标记为carried（前值填充）
        if (state.oiTs != null && (timestamp - state.oiTs) > MINUTE_MS) {
            state.isOiCarried = true;
        }
        
        latestState.update(state);
        
        // 注册下一个分钟定时器
        long nextMinute = timestamp + MINUTE_MS;
        ctx.timerService().registerEventTimeTimer(nextMinute);
        
        if (log.isDebugEnabled()) {
            log.debug("ContextMetrics emitted: symbol={}, mark={}, basis={}, funding={}, oiDelta={}", 
                     symbol, metrics.getMarkPrice(), metrics.getBasisBps(), 
                     metrics.getFundingRate(), metrics.getOiDelta1m());
        }
    }
    
    /**
     * 更新Funding EMA（在线算法，GPT建议的P0修复）
     * 
     * 公式：EMA_t = α · x_t + (1-α) · EMA_{t-1}
     * 其中：α = 1 - exp(-Δt / τ)
     * τ = 24小时 = 86400秒
     * 
     * 优点：
     * - 只需单值状态
     * - 适应不规则更新频率
     * - 对异常值不敏感
     */
    private void updateFundingEMA(LatestState state, BigDecimal newFundingRate, Long currentTs) {
        if (newFundingRate == null || currentTs == null) {
            return;
        }
        
        // τ = 24小时（秒）
        final double TAU_SECONDS = 24 * 3600;
        
        if (state.fundingEma24h == null) {
            // 初始值
            state.fundingEma24h = newFundingRate;
            state.lastEmaUpdateTs = currentTs;
            return;
        }
        
        // 计算Δt（秒）
        long deltaMs = currentTs - (state.lastEmaUpdateTs != null ? state.lastEmaUpdateTs : currentTs);
        double deltaSeconds = deltaMs / 1000.0;
        
        // 计算α
        double alpha = 1 - Math.exp(-deltaSeconds / TAU_SECONDS);
        
        // 更新EMA
        BigDecimal prevEma = state.fundingEma24h;
        BigDecimal newEma = newFundingRate.multiply(BigDecimal.valueOf(alpha))
                .add(prevEma.multiply(BigDecimal.valueOf(1 - alpha)));
        
        state.fundingEma24h = newEma.setScale(8, RoundingMode.HALF_UP);
        state.lastEmaUpdateTs = currentTs;
        
        if (log.isTraceEnabled()) {
            log.trace("Funding EMA updated: prev={}, new={}, alpha={}, result={}", 
                     prevEma, newFundingRate, alpha, state.fundingEma24h);
        }
    }
}


