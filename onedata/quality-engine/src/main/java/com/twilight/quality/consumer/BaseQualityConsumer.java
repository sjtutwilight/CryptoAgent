package com.twilight.quality.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.quality.alert.AlertDispatcher;
import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.metric.QualityMetric;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.RuleEngine;
import com.twilight.quality.sink.QualityMetricSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 质量检测消费者基类
 * 提供通用的消息处理逻辑
 */
public abstract class BaseQualityConsumer {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final ObjectMapper objectMapper = new ObjectMapper();
    
    protected final RuleEngine ruleEngine;
    protected final AlertDispatcher alertDispatcher;
    protected final QualityMetricSink metricSink;
    
    // 统计计数
    protected final AtomicLong processedCount = new AtomicLong(0);
    protected final AtomicLong errorCount = new AtomicLong(0);
    protected final AtomicLong alertCount = new AtomicLong(0);
    
    protected BaseQualityConsumer(RuleEngine ruleEngine, 
                                   AlertDispatcher alertDispatcher,
                                   QualityMetricSink metricSink) {
        this.ruleEngine = ruleEngine;
        this.alertDispatcher = alertDispatcher;
        this.metricSink = metricSink;
    }
    
    /**
     * 处理单条消息
     */
    protected void processMessage(String message, DataDomain domain) {
        try {
            // 1. 解析JSON
            JsonNode jsonNode = objectMapper.readTree(message);
            
            // 2. 构建上下文
            RuleContext context = buildContext(jsonNode, domain);
            
            // 3. 执行规则检测
            List<RuleResult> results = ruleEngine.processMessage(jsonNode, context);
            
            // 4. 处理检测结果
            handleResults(results);
            
            processedCount.incrementAndGet();
            
        } catch (JsonProcessingException e) {
            log.warn("消息解析失败 domain={}: {}", domain, e.getMessage());
            errorCount.incrementAndGet();
        } catch (Exception e) {
            log.error("消息处理异常 domain={}: {}", domain, e.getMessage(), e);
            errorCount.incrementAndGet();
        }
    }
    
    /**
     * 批量处理消息
     */
    protected void processMessages(List<String> messages, DataDomain domain) {
        for (String message : messages) {
            processMessage(message, domain);
        }
        
        // 每1000条记录一次统计
        if (processedCount.get() % 1000 == 0) {
            logStats(domain);
        }
    }
    
    /**
     * 构建规则上下文
     * 子类可覆盖以提供特定的上下文信息
     */
    protected RuleContext buildContext(JsonNode message, DataDomain domain) {
        String streamKey = extractStreamKey(message, domain);
        Long eventTime = extractEventTime(message, domain);
        
        return RuleContext.builder()
                .domain(domain)
                .streamKey(streamKey)
                .receiveTime(Instant.now())
                .eventTime(eventTime)
                .rawMessage(message.toString())
                .build();
    }
    
    /**
     * 提取流标识（子类实现）
     */
    protected abstract String extractStreamKey(JsonNode message, DataDomain domain);
    
    /**
     * 提取事件时间（子类实现）
     */
    protected abstract Long extractEventTime(JsonNode message, DataDomain domain);
    
    /**
     * 处理规则检测结果
     */
    protected void handleResults(List<RuleResult> results) {
        for (RuleResult result : results) {
            // 1. 保存指标
            QualityMetric metric = QualityMetric.fromRuleResult(result);
            metricSink.save(metric);
            
            // 2. 如果检测失败，发送告警
            if (!result.isPassed()) {
                QualityAlert alert = QualityAlert.fromRuleResult(result);
                alertDispatcher.dispatch(alert);
                alertCount.incrementAndGet();
            }
        }
    }
    
    /**
     * 记录统计信息
     */
    protected void logStats(DataDomain domain) {
        log.info("📊 {} 消费统计: 已处理={}, 错误={}, 告警={}",
                domain.getDomainId(), processedCount.get(), errorCount.get(), alertCount.get());
    }
    
    /**
     * 获取处理统计
     */
    public long getProcessedCount() {
        return processedCount.get();
    }
    
    public long getErrorCount() {
        return errorCount.get();
    }
    
    public long getAlertCount() {
        return alertCount.get();
    }
}

