package com.twilight.aggregator.process.kline.indicators;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 技术指标配置类
 * 
 * 用于配置各类技术指标的参数，支持灵活的参数化配置
 */
public class IndicatorConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // 通用参数
    private final Map<String, Object> params;
    
    private IndicatorConfig(Builder builder) {
        this.params = new HashMap<>(builder.params);
    }
    
    public Object getParam(String key) {
        return params.get(key);
    }
    
    public int getIntParam(String key, int defaultValue) {
        Object value = params.get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        }
        return defaultValue;
    }
    
    public double getDoubleParam(String key, double defaultValue) {
        Object value = params.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return defaultValue;
    }
    
    public boolean getBooleanParam(String key, boolean defaultValue) {
        Object value = params.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return defaultValue;
    }
    
    public Map<String, Object> getAllParams() {
        return new HashMap<>(params);
    }
    
    @Override
    public String toString() {
        return "IndicatorConfig{params=" + params + '}';
    }
    
    /**
     * Builder模式构建配置
     */
    public static class Builder {
        private final Map<String, Object> params = new HashMap<>();
        
        public Builder param(String key, Object value) {
            params.put(key, value);
            return this;
        }
        
        public Builder intParam(String key, int value) {
            params.put(key, value);
            return this;
        }
        
        public Builder doubleParam(String key, double value) {
            params.put(key, value);
            return this;
        }
        
        public Builder booleanParam(String key, boolean value) {
            params.put(key, value);
            return this;
        }
        
        public IndicatorConfig build() {
            return new IndicatorConfig(this);
        }
    }
    
    // ========== 预定义配置工厂方法 ==========
    
    /**
     * MACD默认配置：快线12，慢线26，信号线9
     */
    public static IndicatorConfig macdDefault() {
        return new Builder()
            .intParam("fast_period", 12)
            .intParam("slow_period", 26)
            .intParam("signal_period", 9)
            .build();
    }
    
    /**
     * RSI默认配置：周期14
     */
    public static IndicatorConfig rsiDefault() {
        return new Builder()
            .intParam("period", 14)
            .intParam("overbought", 70)  // 超买阈值
            .intParam("oversold", 30)     // 超卖阈值
            .build();
    }
    
    /**
     * 布林带默认配置：周期20，标准差倍数2
     */
    public static IndicatorConfig bollingerDefault() {
        return new Builder()
            .intParam("period", 20)
            .doubleParam("std_dev_multiplier", 2.0)
            .build();
    }
    
    /**
     * KDJ默认配置：K周期9，D周期3，J周期3
     */
    public static IndicatorConfig kdjDefault() {
        return new Builder()
            .intParam("k_period", 9)
            .intParam("d_period", 3)
            .intParam("j_period", 3)
            .intParam("overbought", 80)
            .intParam("oversold", 20)
            .build();
    }
    
    /**
     * ATR默认配置：周期14
     */
    public static IndicatorConfig atrDefault() {
        return new Builder()
            .intParam("period", 14)
            .build();
    }
    
    /**
     * EMA默认配置
     */
    public static IndicatorConfig emaDefault(int period) {
        return new Builder()
            .intParam("period", period)
            .build();
    }
}




