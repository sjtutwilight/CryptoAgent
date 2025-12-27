package com.twilight.aggregator.process.kline.indicators;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.KlineData;
import com.twilight.aggregator.model.KlineSignal;
import com.twilight.aggregator.model.IndicatorMetric;
import com.twilight.aggregator.process.kline.IndicatorOutputTags;

/**
 * 技术指标处理器抽象基类
 * 
 * 提供统一的指标计算框架，所有技术指标处理器都应继承此类
 * 
 * 核心功能：
 * 1. 管理价格队列状态（用于指标计算）
 * 2. 管理指标历史值状态（用于信号判断）
 * 3. 提供模板方法定义统一的处理流程
 * 4. 提供通用工具方法（MA计算、EMA计算等）
 * 
 * 子类需要实现：
 * - getRequiredPeriod(): 返回指标所需的最小数据周期
 * - getStrategyName(): 返回策略名称
 * - calculateIndicator(): 计算指标值
 * - generateSignal(): 根据指标值生成交易信号
 * 
 * @param <T> 指标值类型，必须实现Serializable
 */
public abstract class BaseIndicatorProcessor<T extends java.io.Serializable> 
        extends KeyedProcessFunction<String, KlineData, KlineSignal> {
    
    private static final long serialVersionUID = 1L;
    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    // 状态：价格队列（OHLCV数据）
    protected transient ValueState<PriceQueue> priceQueueState;
    
    // 状态：上一次的指标值
    protected transient ValueState<T> lastIndicatorState;
    
    // 配置参数
    protected final IndicatorConfig config;
    
    /**
     * 构造函数
     * @param config 指标配置
     */
    protected BaseIndicatorProcessor(IndicatorConfig config) {
        this.config = config;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化价格队列状态
        ValueStateDescriptor<PriceQueue> priceQueueDescriptor = 
            new ValueStateDescriptor<>(
                "price-queue-" + getStrategyName(),
                TypeInformation.of(new TypeHint<PriceQueue>() {})
            );
        priceQueueState = getRuntimeContext().getState(priceQueueDescriptor);
        
        // 初始化指标值状态
        ValueStateDescriptor<T> indicatorDescriptor = 
            new ValueStateDescriptor<>(
                "indicator-" + getStrategyName(),
                getIndicatorTypeInformation()
            );
        lastIndicatorState = getRuntimeContext().getState(indicatorDescriptor);
        
        log.info("{} initialized with config: {}", getStrategyName(), config);
    }
    
    /**
     * 模板方法：统一的处理流程
     */
    @Override
    public void processElement(
            KlineData klineData,
            Context ctx,
            Collector<KlineSignal> out) throws Exception {
        
        // 1. 验证K线数据有效性
        if (!isValidKlineData(klineData)) {
            return;
        }
        
        // 2. 获取或初始化价格队列
        PriceQueue priceQueue = priceQueueState.value();
        if (priceQueue == null) {
            priceQueue = new PriceQueue(getRequiredPeriod());
        }
        
        // 3. 添加新K线数据到队列
        priceQueue.add(klineData);
        priceQueueState.update(priceQueue);
        
        // 4. 检查数据是否足够
        if (priceQueue.size() < getRequiredPeriod()) {
            log.debug("{}: Insufficient data for {}, {}/{} collected", 
                     getStrategyName(), klineData.getSymbol(), 
                     priceQueue.size(), getRequiredPeriod());
            return;
        }
        
        // 5. 计算指标值
        T currentIndicator = calculateIndicator(priceQueue, klineData);
        if (currentIndicator == null) {
            log.warn("{}: Failed to calculate indicator for {}", 
                    getStrategyName(), klineData.getSymbol());
            return;
        }
        
        // 6. 获取历史指标值
        T lastIndicator = lastIndicatorState.value();
        
        // 7. 生成交易信号
        KlineSignal signal = null;
        if (lastIndicator != null) {
            signal = generateSignal(
                currentIndicator, 
                lastIndicator, 
                klineData, 
                priceQueue
            );
            
            if (signal != null) {
                out.collect(signal);
                log.info("{}: Generated signal for {}: {} at price {}", 
                         getStrategyName(),
                         signal.getSymbol(), 
                         signal.getSignalType(), 
                         signal.getCurrentPrice());
            }
        } else {
            log.debug("{}: First calculation for {}, no signal generated", 
                     getStrategyName(), klineData.getSymbol());
        }
        
        // 8. 更新指标状态
        lastIndicatorState.update(currentIndicator);

        // 9. 输出指标快照
        IndicatorMetric metric = buildIndicatorMetric(currentIndicator, klineData, priceQueue, signal);
        if (metric != null) {
            ctx.output(IndicatorOutputTags.INDICATOR_METRICS_TAG, metric);
        }
    }
    
    /**
     * 验证K线数据有效性
     */
    protected boolean isValidKlineData(KlineData klineData) {
        if (klineData == null || klineData.getKline() == null) {
            log.warn("{}: Null kline data", getStrategyName());
            return false;
        }
        
        BigDecimal closePrice = klineData.getClosePrice();
        if (closePrice == null || closePrice.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("{}: Invalid close price for {}: {}", 
                    getStrategyName(), klineData.getSymbol(), closePrice);
            return false;
        }
        
        return true;
    }
    
    // ========== 抽象方法：子类必须实现 ==========
    
    /**
     * 获取指标所需的最小数据周期
     */
    protected abstract int getRequiredPeriod();
    
    /**
     * 获取策略名称
     */
    protected abstract String getStrategyName();
    
    /**
     * 获取指标值的TypeInformation（用于状态序列化）
     */
    protected abstract TypeInformation<T> getIndicatorTypeInformation();
    
    /**
     * 计算指标值
     * @param priceQueue 价格队列
     * @param currentKline 当前K线
     * @return 指标值，计算失败返回null
     */
    protected abstract T calculateIndicator(PriceQueue priceQueue, KlineData currentKline);
    
    /**
     * 根据指标值生成交易信号
     * @param current 当前指标值
     * @param last 上一次指标值
     * @param klineData 当前K线数据
     * @param priceQueue 价格队列（用于获取额外信息）
     * @return 交易信号，不生成信号返回null
     */
    protected abstract KlineSignal generateSignal(
        T current, 
        T last, 
        KlineData klineData,
        PriceQueue priceQueue
    );

    /**
     * 构建指标快照，供下游持久化
     */
    protected abstract IndicatorMetric buildIndicatorMetric(
        T currentIndicator,
        KlineData klineData,
        PriceQueue priceQueue,
        KlineSignal currentSignal
    );
    
    // ========== 通用工具方法 ==========
    
    /**
     * 计算简单移动平均（SMA）
     * @param values 数值队列
     * @param period 周期
     * @return SMA值
     */
    protected BigDecimal calculateSMA(Deque<BigDecimal> values, int period) {
        if (values.size() < period) {
            return null;
        }
        
        BigDecimal sum = BigDecimal.ZERO;
        int count = 0;
        
        for (BigDecimal value : values) {
            if (count >= values.size() - period) {
                sum = sum.add(value);
            }
            count++;
        }
        
        return sum.divide(BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP);
    }
    
    /**
     * 计算指数移动平均（EMA）
     * @param currentPrice 当前价格
     * @param previousEMA 前一个EMA值
     * @param period 周期
     * @return EMA值
     */
    protected BigDecimal calculateEMA(BigDecimal currentPrice, BigDecimal previousEMA, int period) {
        if (previousEMA == null) {
            return currentPrice;
        }
        
        // EMA = Price * k + EMA(previous) * (1 - k)
        // k = 2 / (period + 1)
        BigDecimal k = BigDecimal.valueOf(2)
            .divide(BigDecimal.valueOf(period + 1), 8, RoundingMode.HALF_UP);
        
        return currentPrice.multiply(k)
            .add(previousEMA.multiply(BigDecimal.ONE.subtract(k)))
            .setScale(8, RoundingMode.HALF_UP);
    }
    
    /**
     * 计算标准差
     * @param values 数值队列
     * @param period 周期
     * @param mean 均值（如果已计算）
     * @return 标准差
     */
    protected BigDecimal calculateStdDev(Deque<BigDecimal> values, int period, BigDecimal mean) {
        if (values.size() < period) {
            return null;
        }
        
        if (mean == null) {
            mean = calculateSMA(values, period);
        }
        
        BigDecimal sumSquaredDiff = BigDecimal.ZERO;
        int count = 0;
        
        for (BigDecimal value : values) {
            if (count >= values.size() - period) {
                BigDecimal diff = value.subtract(mean);
                sumSquaredDiff = sumSquaredDiff.add(diff.multiply(diff));
            }
            count++;
        }
        
        BigDecimal variance = sumSquaredDiff.divide(
            BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP
        );
        
        // 使用牛顿迭代法计算平方根
        return sqrt(variance);
    }
    
    /**
     * 牛顿迭代法计算平方根
     */
    protected BigDecimal sqrt(BigDecimal value) {
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        
        BigDecimal x = value;
        BigDecimal lastX;
        
        // 迭代计算
        for (int i = 0; i < 20; i++) {
            lastX = x;
            x = value.divide(x, 8, RoundingMode.HALF_UP)
                .add(x)
                .divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_UP);
            
            // 收敛判断
            if (x.subtract(lastX).abs().compareTo(BigDecimal.valueOf(0.00000001)) < 0) {
                break;
            }
        }
        
        return x.setScale(8, RoundingMode.HALF_UP);
    }
    
    /**
     * 判断是否发生上穿（金叉）
     */
    protected boolean isCrossUp(BigDecimal lastFast, BigDecimal currentFast, 
                                BigDecimal lastSlow, BigDecimal currentSlow) {
        return lastFast.compareTo(lastSlow) <= 0 && currentFast.compareTo(currentSlow) > 0;
    }
    
    /**
     * 判断是否发生下穿（死叉）
     */
    protected boolean isCrossDown(BigDecimal lastFast, BigDecimal currentFast,
                                  BigDecimal lastSlow, BigDecimal currentSlow) {
        return lastFast.compareTo(lastSlow) >= 0 && currentFast.compareTo(currentSlow) < 0;
    }
    
    /**
     * 价格队列数据结构
     * 存储OHLCV数据，支持多种计算需求
     */
    public static class PriceQueue implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        private final int maxSize;
        private final Deque<BigDecimal> closePrices;
        private final Deque<BigDecimal> highPrices;
        private final Deque<BigDecimal> lowPrices;
        private final Deque<BigDecimal> openPrices;
        private final Deque<BigDecimal> volumes;
        
        public PriceQueue(int maxSize) {
            this.maxSize = maxSize;
            this.closePrices = new ArrayDeque<>(maxSize);
            this.highPrices = new ArrayDeque<>(maxSize);
            this.lowPrices = new ArrayDeque<>(maxSize);
            this.openPrices = new ArrayDeque<>(maxSize);
            this.volumes = new ArrayDeque<>(maxSize);
        }
        
        public void add(KlineData klineData) {
            KlineData.Kline kline = klineData.getKline();
            
            closePrices.addLast(kline.getClosePrice());
            highPrices.addLast(kline.getHighPrice());
            lowPrices.addLast(kline.getLowPrice());
            openPrices.addLast(kline.getOpenPrice());
            volumes.addLast(kline.getBaseVolume());
            
            // 保持队列长度
            while (closePrices.size() > maxSize) {
                closePrices.removeFirst();
                highPrices.removeFirst();
                lowPrices.removeFirst();
                openPrices.removeFirst();
                volumes.removeFirst();
            }
        }
        
        public int size() {
            return closePrices.size();
        }
        
        public Deque<BigDecimal> getClosePrices() {
            return closePrices;
        }
        
        public Deque<BigDecimal> getHighPrices() {
            return highPrices;
        }
        
        public Deque<BigDecimal> getLowPrices() {
            return lowPrices;
        }
        
        public Deque<BigDecimal> getOpenPrices() {
            return openPrices;
        }
        
        public Deque<BigDecimal> getVolumes() {
            return volumes;
        }
        
        public BigDecimal getLastClose() {
            return closePrices.isEmpty() ? null : closePrices.peekLast();
        }
        
        public BigDecimal getLastHigh() {
            return highPrices.isEmpty() ? null : highPrices.peekLast();
        }
        
        public BigDecimal getLastLow() {
            return lowPrices.isEmpty() ? null : lowPrices.peekLast();
        }
        
        public BigDecimal getLastOpen() {
            return openPrices.isEmpty() ? null : openPrices.peekLast();
        }
        
        public BigDecimal getLastVolume() {
            return volumes.isEmpty() ? null : volumes.peekLast();
        }
    }
}




