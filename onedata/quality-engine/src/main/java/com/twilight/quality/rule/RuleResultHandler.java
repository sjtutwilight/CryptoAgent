package com.twilight.quality.rule;

import com.twilight.quality.alert.AlertDispatcher;
import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.metric.QualityMetric;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.sink.QualityMetricSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 规则结果处理器
 * 处理规则检测结果：保存指标、发送告警
 */
@Component
public class RuleResultHandler {
    
    private static final Logger log = LoggerFactory.getLogger(RuleResultHandler.class);
    
    private final QualityMetricSink metricSink;
    private final AlertDispatcher alertDispatcher;
    
    public RuleResultHandler(QualityMetricSink metricSink, AlertDispatcher alertDispatcher) {
        this.metricSink = metricSink;
        this.alertDispatcher = alertDispatcher;
    }
    
    /**
     * 处理规则结果
     */
    public void handleResult(RuleResult result) {
        if (result == null) {
            return;
        }
        
        // 1. 保存指标
        QualityMetric metric = QualityMetric.fromRuleResult(result);
        metricSink.save(metric);
        
        // 2. 如果检测失败，发送告警
        if (!result.isPassed()) {
            QualityAlert alert = QualityAlert.fromRuleResult(result);
            alertDispatcher.dispatch(alert);
            
            log.debug("规则检测失败: {} - {} - {}", 
                    result.getRuleName(), result.getDomain(), result.getMessage());
        }
    }
}

