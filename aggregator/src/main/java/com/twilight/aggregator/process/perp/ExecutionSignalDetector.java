package com.twilight.aggregator.process.perp;

import java.math.BigDecimal;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.twilight.aggregator.model.perp.ExecutionMetrics;
import com.twilight.aggregator.model.perp.PerpSignal;
import com.twilight.aggregator.model.perp.PerpSignal.SignalType;
import com.twilight.aggregator.model.perp.PerpSignal.SignalLevel;

/**
 * 执行健康度信号检测器（快流）
 * 
 * 检测规则：
 * 1. 点差异常：spread > 硬编码阈值（后续可改为分位数）
 * 2. 深度骤降：depth < 硬编码阈值
 * 3. 冲击成本过高：impact > 硬编码阈值
 * 
 * 注意：目前使用硬编码阈值，生产环境应使用滚动分位数或从Redis读取动态阈值
 */
public class ExecutionSignalDetector extends ProcessFunction<ExecutionMetrics, PerpSignal> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(ExecutionSignalDetector.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // 算法版本
    private static final String ALGO_VERSION = "v1.0";
    
    // ===== 硬编码阈值（P0实现，后续优化为动态分位数）=====
    
    // 点差阈值（基点）
    private static final double SPREAD_WARN_BPS = 10.0;   // 警告阈值
    private static final double SPREAD_CRITICAL_BPS = 20.0; // 严重阈值
    
    // 深度阈值（USD）
    private static final double DEPTH_50K_WARN = 30000.0;   // 深度 < 30k 警告
    private static final double DEPTH_50K_CRITICAL = 10000.0; // 深度 < 10k 严重
    
    // 冲击成本阈值（基点）
    private static final double IMPACT_50K_WARN_BPS = 30.0;   // 30bps警告
    private static final double IMPACT_50K_CRITICAL_BPS = 50.0; // 50bps严重
    
    @Override
    public void processElement(ExecutionMetrics metrics, Context ctx, Collector<PerpSignal> out) 
            throws Exception {
        
        // 1. 检测点差异常
        if (metrics.getSpreadBps() != null) {
            double spreadBps = metrics.getSpreadBps();
            if (spreadBps > SPREAD_CRITICAL_BPS) {
                out.collect(buildSignal(
                    metrics,
                    SignalType.EXEC_HEALTH,
                    SignalLevel.CRITICAL,
                    "spread_anomaly",
                    spreadBps,
                    SPREAD_CRITICAL_BPS,
                    String.format("点差异常严重：%.2f bps，超过阈值 %.2f bps", spreadBps, SPREAD_CRITICAL_BPS)
                ));
            } else if (spreadBps > SPREAD_WARN_BPS) {
                out.collect(buildSignal(
                    metrics,
                    SignalType.EXEC_HEALTH,
                    SignalLevel.WARNING,
                    "spread_anomaly",
                    spreadBps,
                    SPREAD_WARN_BPS,
                    String.format("点差偏高：%.2f bps，超过阈值 %.2f bps", spreadBps, SPREAD_WARN_BPS)
                ));
            }
        }
        
        // 2. 检测深度骤降
        if (metrics.getDepth50k() != null) {
            double depth50k = metrics.getDepth50k().doubleValue();
            if (depth50k < DEPTH_50K_CRITICAL) {
                out.collect(buildSignal(
                    metrics,
                    SignalType.EXEC_HEALTH,
                    SignalLevel.CRITICAL,
                    "depth_thin",
                    depth50k,
                    DEPTH_50K_CRITICAL,
                    String.format("流动性极度枯竭：50k深度仅 $%.0f，低于阈值 $%.0f", depth50k, DEPTH_50K_CRITICAL)
                ));
            } else if (depth50k < DEPTH_50K_WARN) {
                out.collect(buildSignal(
                    metrics,
                    SignalType.EXEC_HEALTH,
                    SignalLevel.WARNING,
                    "depth_thin",
                    depth50k,
                    DEPTH_50K_WARN,
                    String.format("流动性偏薄：50k深度 $%.0f，低于阈值 $%.0f", depth50k, DEPTH_50K_WARN)
                ));
            }
        }
        
        // 3. 检测冲击成本过高
        if (metrics.getImpact50kBps() != null) {
            double impact50k = metrics.getImpact50kBps();
            if (impact50k > IMPACT_50K_CRITICAL_BPS) {
                out.collect(buildSignal(
                    metrics,
                    SignalType.EXEC_HEALTH,
                    SignalLevel.CRITICAL,
                    "high_impact",
                    impact50k,
                    IMPACT_50K_CRITICAL_BPS,
                    String.format("冲击成本极高：50k冲击 %.2f bps，超过阈值 %.2f bps", impact50k, IMPACT_50K_CRITICAL_BPS)
                ));
            } else if (impact50k > IMPACT_50K_WARN_BPS) {
                out.collect(buildSignal(
                    metrics,
                    SignalType.EXEC_HEALTH,
                    SignalLevel.WARNING,
                    "high_impact",
                    impact50k,
                    IMPACT_50K_WARN_BPS,
                    String.format("冲击成本偏高：50k冲击 %.2f bps，超过阈值 %.2f bps", impact50k, IMPACT_50K_WARN_BPS)
                ));
            }
        }
    }
    
    /**
     * 构建信号对象
     */
    private PerpSignal buildSignal(ExecutionMetrics metrics, SignalType type, SignalLevel level,
                                   String metricName, double metricValue, double threshold,
                                   String detail) {
        // 构建上下文JSON（包含相关指标快照）
        ObjectNode context = objectMapper.createObjectNode();
        context.put("mid_price", metrics.getMidPrice() != null ? metrics.getMidPrice().doubleValue() : 0);
        context.put("spread_bps", metrics.getSpreadBps() != null ? metrics.getSpreadBps() : 0);
        context.put("depth_50k", metrics.getDepth50k() != null ? metrics.getDepth50k().doubleValue() : 0);
        context.put("impact_50k_bps", metrics.getImpact50kBps() != null ? metrics.getImpact50kBps() : 0);
        context.put("volume_usd", metrics.getVolumeUsd() != null ? metrics.getVolumeUsd().doubleValue() : 0);
        context.put("imbalance_top5", metrics.getImbalanceTop5() != null ? metrics.getImbalanceTop5() : 0);
        
        return PerpSignal.builder()
                .symbol(metrics.getSymbol())
                .exchange(metrics.getExchange())
                .signalTime(metrics.getEndTime())
                .signalType(type)
                .signalLevel(level)
                .metricName(metricName)
                .metricValue(metricValue)
                .threshold(threshold)
                .signalDetail(detail)
                .contextJson(context.toString())
                .algoVersion(ALGO_VERSION)
                .processTime(System.currentTimeMillis())
                .build();
    }
}







