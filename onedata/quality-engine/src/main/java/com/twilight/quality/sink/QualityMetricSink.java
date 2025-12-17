package com.twilight.quality.sink;

import com.twilight.quality.domain.metric.QualityMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 质量指标落库组件
 * 批量写入ClickHouse
 */
@Component
public class QualityMetricSink {
    
    private static final Logger log = LoggerFactory.getLogger(QualityMetricSink.class);
    
    private static final String INSERT_SQL = 
            "INSERT INTO quality_metrics " +
            "(metric_id, domain, stream_key, dimension, rule_name, " +
            "value, threshold, passed, window_start, window_end, message_count, collected_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    private final DataSource clickHouseDataSource;
    
    @Value("${quality.metric.batch-size:100}")
    private int batchSize;
    
    @Value("${quality.metric.flush-interval-ms:5000}")
    private long flushIntervalMs;
    
    /**
     * 待写入队列
     */
    private final BlockingQueue<QualityMetric> queue = new LinkedBlockingQueue<>(10000);
    
    /**
     * 统计计数
     */
    private final AtomicLong savedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    
    public QualityMetricSink(@Qualifier("clickHouseDataSource") DataSource clickHouseDataSource) {
        this.clickHouseDataSource = clickHouseDataSource;
    }
    
    /**
     * 保存指标（异步）
     */
    public void save(QualityMetric metric) {
        if (!queue.offer(metric)) {
            log.warn("指标队列已满，丢弃指标: {}", metric.getRuleName());
        }
    }
    
    /**
     * 定时刷新队列
     */
    @Scheduled(fixedDelayString = "${quality.metric.flush-interval-ms:5000}")
    public void flush() {
        List<QualityMetric> batch = new ArrayList<>();
        queue.drainTo(batch, batchSize);
        
        if (batch.isEmpty()) {
            return;
        }
        
        try {
            writeBatch(batch);
            savedCount.addAndGet(batch.size());
            log.debug("写入 {} 条质量指标", batch.size());
        } catch (Exception e) {
            errorCount.addAndGet(batch.size());
            log.error("写入质量指标失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 批量写入
     */
    private void writeBatch(List<QualityMetric> metrics) throws Exception {
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
            
            for (QualityMetric metric : metrics) {
                stmt.setString(1, metric.getMetricId());
                stmt.setString(2, metric.getDomain() != null ? metric.getDomain().getDomainId() : null);
                stmt.setString(3, metric.getStreamKey());
                stmt.setString(4, metric.getDimension() != null ? metric.getDimension().getCode() : null);
                stmt.setString(5, metric.getRuleName());
                stmt.setObject(6, metric.getValue());
                stmt.setObject(7, metric.getThreshold());
                stmt.setBoolean(8, metric.getPassed() != null && metric.getPassed());
                stmt.setTimestamp(9, metric.getWindowStart() != null 
                        ? new Timestamp(metric.getWindowStart()) : null);
                stmt.setTimestamp(10, metric.getWindowEnd() != null 
                        ? new Timestamp(metric.getWindowEnd()) : null);
                stmt.setObject(11, metric.getMessageCount());
                stmt.setTimestamp(12, metric.getCollectedAt() != null 
                        ? Timestamp.from(metric.getCollectedAt()) : new Timestamp(System.currentTimeMillis()));
                
                stmt.addBatch();
            }
            
            stmt.executeBatch();
        }
    }
    
    /**
     * 关闭时刷新剩余数据
     */
    @PreDestroy
    public void shutdown() {
        log.info("关闭指标落库组件，刷新剩余数据...");
        while (!queue.isEmpty()) {
            flush();
        }
        log.info("指标落库统计: 已保存={}, 错误={}", savedCount.get(), errorCount.get());
    }
    
    /**
     * 获取统计信息
     */
    public long getSavedCount() {
        return savedCount.get();
    }
    
    public long getErrorCount() {
        return errorCount.get();
    }
    
    public int getQueueSize() {
        return queue.size();
    }
}

