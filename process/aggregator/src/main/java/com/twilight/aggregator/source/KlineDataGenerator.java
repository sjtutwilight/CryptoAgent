package com.twilight.aggregator.source;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.KlineData;

/**
 * K线数据生成器 - 用于测试和开发
 * 
 * 功能：
 * - 生成模拟的K线数据用于测试MA策略
 * - 支持配置生成速率和趋势模式
 * - 模拟真实的价格波动
 */
public class KlineDataGenerator implements SourceFunction<KlineData> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(KlineDataGenerator.class);
    
    private volatile boolean running = true;
    private final String exchange;
    private final String symbol;
    private final String interval;
    private final long intervalMs;
    private final int generateIntervalMs; // 数据生成间隔
    private final TrendType trendType;
    
    // 价格生成参数
    private BigDecimal basePrice;
    private final BigDecimal volatility; // 波动率
    private final Random random;
    
    /**
     * 趋势类型
     */
    public enum TrendType {
        UPTREND,      // 上涨趋势
        DOWNTREND,    // 下跌趋势
        SIDEWAYS,     // 横盘
        RANDOM        // 随机波动
    }
    
    /**
     * 构造函数 - 默认参数
     */
    public KlineDataGenerator() {
        this("binance", "BTCUSDT", "1m", TrendType.UPTREND);
    }
    
    /**
     * 构造函数 - 自定义参数
     */
    public KlineDataGenerator(String exchange, String symbol, String interval, TrendType trendType) {
        this.exchange = exchange;
        this.symbol = symbol;
        this.interval = interval;
        this.intervalMs = parseInterval(interval);
        this.generateIntervalMs = 1000; // 每秒生成一根K线（加快测试）
        this.trendType = trendType;
        this.basePrice = new BigDecimal("42000.00"); // BTC基础价格
        this.volatility = new BigDecimal("0.002"); // 0.2%波动率
        this.random = new Random();
        
        log.info("KlineDataGenerator initialized: exchange={}, symbol={}, interval={}, trend={}", 
                 exchange, symbol, interval, trendType);
    }
    
    @Override
    public void run(SourceContext<KlineData> ctx) throws Exception {
        log.info("🚀 Starting Kline data generation...");
        
        long currentTime = System.currentTimeMillis();
        long klineStartTime = (currentTime / intervalMs) * intervalMs; // 对齐到K线周期
        int count = 0;
        
        while (running) {
            try {
                // 生成K线数据
                KlineData klineData = generateKline(klineStartTime);
                
                // 发送数据
                ctx.collect(klineData);
                
                count++;
                if (count % 10 == 0) {
                    log.info("✅ Generated {} klines, latest price: {}", 
                             count, klineData.getClosePrice());
                }
                
                // 更新价格（模拟趋势）
                updateBasePrice();
                
                // 移动到下一个K线周期
                klineStartTime += intervalMs;
                
                // 控制生成速率
                Thread.sleep(generateIntervalMs);
                
            } catch (InterruptedException e) {
                log.warn("Generator interrupted");
                break;
            } catch (Exception e) {
                log.error("Error generating kline data: {}", e.getMessage(), e);
            }
        }
        
        log.info("🛑 Kline data generation stopped. Total generated: {}", count);
    }
    
    @Override
    public void cancel() {
        log.info("Cancelling Kline data generator...");
        running = false;
    }
    
    /**
     * 生成单个K线数据
     */
    private KlineData generateKline(long startTime) {
        KlineData klineData = new KlineData();
        klineData.setExchange(exchange);
        klineData.setSymbol(symbol);
        klineData.setInterval(interval);
        klineData.setEventTime(startTime + intervalMs);
        klineData.setIngestTime(System.currentTimeMillis());
        
        // 生成OHLC数据
        KlineData.Kline kline = new KlineData.Kline();
        kline.setStartTime(startTime);
        kline.setCloseTime(startTime + intervalMs - 1);
        
        // 生成价格（基于基础价格和波动率）
        BigDecimal open = generatePrice(basePrice);
        BigDecimal close = generatePrice(open);
        BigDecimal high = open.max(close).add(generateRandomChange(basePrice, 0.5));
        BigDecimal low = open.min(close).subtract(generateRandomChange(basePrice, 0.5));
        
        kline.setOpenPrice(open);
        kline.setClosePrice(close);
        kline.setHighPrice(high);
        kline.setLowPrice(low);
        
        // 生成成交量（随机）
        BigDecimal baseVolume = new BigDecimal(String.valueOf(10 + random.nextDouble() * 90));
        BigDecimal quoteVolume = baseVolume.multiply(close);
        kline.setBaseVolume(baseVolume);
        kline.setQuoteVolume(quoteVolume);
        kline.setTradeCount(random.nextInt(500) + 100);
        
        // K线已完成
        kline.setClosed(true);
        
        klineData.setKline(kline);
        
        return klineData;
    }
    
    /**
     * 根据趋势更新基础价格
     */
    private void updateBasePrice() {
        BigDecimal change;
        
        switch (trendType) {
            case UPTREND:
                // 上涨趋势：60%概率上涨
                change = random.nextDouble() < 0.6 
                    ? generateRandomChange(basePrice, 1.0) 
                    : generateRandomChange(basePrice, 1.0).negate();
                break;
                
            case DOWNTREND:
                // 下跌趋势：60%概率下跌
                change = random.nextDouble() < 0.6 
                    ? generateRandomChange(basePrice, 1.0).negate()
                    : generateRandomChange(basePrice, 1.0);
                break;
                
            case SIDEWAYS:
                // 横盘：小幅波动
                change = generateRandomChange(basePrice, 0.3);
                if (random.nextBoolean()) {
                    change = change.negate();
                }
                break;
                
            case RANDOM:
            default:
                // 随机波动
                change = generateRandomChange(basePrice, 1.0);
                if (random.nextBoolean()) {
                    change = change.negate();
                }
                break;
        }
        
        basePrice = basePrice.add(change);
        
        // 确保价格不会太低
        if (basePrice.compareTo(new BigDecimal("30000")) < 0) {
            basePrice = new BigDecimal("30000");
        }
        // 确保价格不会太高
        if (basePrice.compareTo(new BigDecimal("60000")) > 0) {
            basePrice = new BigDecimal("60000");
        }
    }
    
    /**
     * 生成价格（基于基础价格和随机波动）
     */
    private BigDecimal generatePrice(BigDecimal base) {
        BigDecimal change = generateRandomChange(base, 1.0);
        if (random.nextBoolean()) {
            change = change.negate();
        }
        return base.add(change);
    }
    
    /**
     * 生成随机变化量
     */
    private BigDecimal generateRandomChange(BigDecimal base, double multiplier) {
        double randomFactor = random.nextDouble() * multiplier;
        return base.multiply(volatility)
                   .multiply(BigDecimal.valueOf(randomFactor))
                   .setScale(2, RoundingMode.HALF_UP);
    }
    
    /**
     * 解析时间间隔字符串为毫秒
     */
    private long parseInterval(String interval) {
        try {
            String unit = interval.substring(interval.length() - 1);
            int value = Integer.parseInt(interval.substring(0, interval.length() - 1));
            
            switch (unit.toLowerCase()) {
                case "s":
                    return value * 1000L;
                case "m":
                    return value * 60 * 1000L;
                case "h":
                    return value * 60 * 60 * 1000L;
                case "d":
                    return value * 24 * 60 * 60 * 1000L;
                default:
                    log.warn("Unknown interval unit: {}, using 1 minute", unit);
                    return 60 * 1000L;
            }
        } catch (Exception e) {
            log.warn("Failed to parse interval: {}, using 1 minute", interval);
            return 60 * 1000L;
        }
    }
}








