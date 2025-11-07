package com.twilight.aggregator.model.perp;

import java.io.Serializable;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * 永续合约信号数据模型（对应ClickHouse表：perp_signals）
 * 
 * 信号类型：
 * - EXEC_HEALTH: 执行健康度信号（spread异常、depth骤降、impact过高）
 * - CROWDING: 拥挤度信号（funding极端、basis异常）
 * - LIQUIDATION_RISK: 清算风险信号（OI激增+薄流动性）
 * 
 * 信号级别：
 * - INFO: 信息提示
 * - WARNING: 警告
 * - CRITICAL: 严重
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PerpSignal implements Serializable {
    private static final long serialVersionUID = 1L;

    // ===== 基础维度 =====
    
    private String symbol;
    private String exchange;
    private Long signalTime;  // 信号产生时间（毫秒）
    
    // 信号类型
    private SignalType signalType;
    
    // 信号级别
    private SignalLevel signalLevel;
    
    // ===== 信号内容 =====
    
    // 指标名称（如：spread_anomaly, depth_thin, funding_extreme）
    private String metricName;
    
    // 指标值
    private Double metricValue;
    
    // 阈值
    private Double threshold;
    
    // 信号描述
    private String signalDetail;
    
    // 完整上下文（JSON格式，包含相关指标快照）
    private String contextJson;
    
    // ===== 元数据 =====
    
    // 算法版本
    private String algoVersion;
    
    // 计算时间
    private Long processTime;
    
    /**
     * 信号类型枚举
     */
    public enum SignalType {
        EXEC_HEALTH,        // 执行健康度
        CROWDING,           // 拥挤度
        LIQUIDATION_RISK    // 清算风险
    }
    
    /**
     * 信号级别枚举
     */
    public enum SignalLevel {
        INFO,      // 信息
        WARNING,   // 警告
        CRITICAL   // 严重
    }
}




