package com.twilight.aggregator.process.token;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.TimeDomain;
import com.twilight.aggregator.model.TokenRecentMetric;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * Hierarchical window aggregator for token metrics.
 * Uses state to maintain historical 20s metrics and latest aggregated metrics
 * for incremental computation.
 */
public class TokenSlidingHierarchicalAggregator
        extends KeyedProcessFunction<String, TokenRecentMetric, TokenRecentMetric> {
    private static final Logger log = LoggerFactory.getLogger(TokenSlidingHierarchicalAggregator.class);
    private static final long serialVersionUID = 1L;
    private final String windowName;

    // State to store 20s metrics for the last hour
    private transient MapState<String, TokenRecentMetric> twentySecMetrics;

    // State to store latest aggregated metrics for each window level and tag
    private transient MapState<String, TokenRecentMetric> latestMetrics;

    public TokenSlidingHierarchicalAggregator(String windowName) {
        this.windowName = windowName;
        log.info("Initializing TokenSlidingHierarchicalAggregator with window name: {}", windowName);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("Opening TokenSlidingHierarchicalAggregator with parameters: {}", parameters);

        // Initialize state for 20s metrics
        MapStateDescriptor<String, TokenRecentMetric> twentySecMetricsDescriptor = new MapStateDescriptor<>(
                "twentySecMetrics", String.class, TokenRecentMetric.class);
        this.twentySecMetrics = getRuntimeContext().getMapState(twentySecMetricsDescriptor);

        // Initialize state for latest aggregated metrics
        MapStateDescriptor<String, TokenRecentMetric> latestMetricsDescriptor = new MapStateDescriptor<>(
                "latestMetrics", String.class, TokenRecentMetric.class);
        this.latestMetrics = getRuntimeContext().getMapState(latestMetricsDescriptor);

        log.debug("State initialized successfully");
    }

    @Override
    public void processElement(TokenRecentMetric newMetric, Context ctx, Collector<TokenRecentMetric> out)
            throws Exception {
        long currentTime = newMetric.getEndTime();
        String tag = newMetric.getTag();
        log.debug("Processing new metric at timestamp: {} for token: {} with tag: {}",
                currentTime, newMetric.getTokenId(), tag);

        try {
            // 使用时间戳和标签作为键
            String timeKey = currentTime + ":" + tag;

            // Store the new 20s metric
            twentySecMetrics.put(timeKey, newMetric);
            log.trace("Stored new metric in state at timestamp: {} with tag: {}", currentTime, tag);

            // 使用窗口名称和标签作为键
            String windowKey = windowName + ":" + tag;

            // Get the previous aggregated metric
            TokenRecentMetric latestMetric = latestMetrics.get(windowKey);
            if (latestMetric == null) {
                log.debug("No previous metric found for window: {} and tag: {}, creating new one", windowName, tag);
                latestMetric = new TokenRecentMetric(newMetric.getTokenId(), windowName, 0, tag);
            }

            // Get the metric that is sliding out
            long slideOutTime = getSlideOutTime(windowName, currentTime);
            String slideOutKey = slideOutTime + ":" + tag;
            TokenRecentMetric slideOutMetric = twentySecMetrics.get(slideOutKey);

            // Compute incremental metric
            TokenRecentMetric result = computeIncrementalMetric(latestMetric, newMetric, slideOutMetric);
            result.setEndTime(currentTime);
            result.setTimeWindow(windowName);
            result.setTag(tag);
            result.setTokenPriceUsd(newMetric.getTokenPriceUsd()); // Use latest price
            result.setMcapUsd(newMetric.getMcapUsd());
            result.setFdvUsd(newMetric.getFdvUsd());
            result.setLiquidityUsd(newMetric.getLiquidityUsd());
            // Update latest metric state
            latestMetrics.put(windowKey, result);
            log.debug("Updated latest metric for window: {} and tag: {}, {}", windowName, tag, result);

            // Register cleanup timer
            long cleanupTime = currentTime + 3_600_000 + 30_000;
            ctx.timerService().registerProcessingTimeTimer(cleanupTime);
            
            // Register output timer based on window type
            long outputInterval = getOutputInterval();
            long nextOutputTime = ((currentTime / outputInterval) + 1) * outputInterval;
            ctx.timerService().registerEventTimeTimer(nextOutputTime);

            log.debug("Updated aggregated metric for token {} with tag {}: volume={}, price={}, buyPressure={}",
                    result.getTokenId(), tag, result.getVolumeUsd(), result.getTokenPriceUsd(),
                    result.getBuyPressureUsd());
            
            // 不立即输出，而是等待定时器触发
            // out.collect(result);

        } catch (Exception e) {
            log.error("Error processing metric for token {} with tag {} at time {}: {}",
                    newMetric.getTokenId(), tag, currentTime, e.getMessage(), e);
            throw e;
        }
    }

    private long getSlideOutTime(String windowName, long currentTime) {
        long slideOutTime;
        switch (windowName) {
            case "1min":
                slideOutTime = currentTime - 20_000; // 20 seconds
                break;
            case "5min":
                slideOutTime = currentTime - 300_000; // 5 minutes
                break;
            case "1h":
                slideOutTime = currentTime - 3_600_000; // 30 minutes
                break;
            default:
                slideOutTime = currentTime;
        }
        log.debug("Calculated slide-out time for window {}: {}", windowName, slideOutTime);
        return slideOutTime;
    }
    
    private long getOutputInterval() {
        switch (windowName) {
            case "1min":
                return 60_000; // 1 minute
            case "5min":
                return 300_000; // 5 minutes
            case "1h":
                return 3_600_000; // 1 hour
            default:
                return 60_000; // default to 1 minute
        }
    }

    private TokenRecentMetric computeIncrementalMetric(TokenRecentMetric latest, TokenRecentMetric newMetric,
            TokenRecentMetric slideOut) {
        log.debug("Computing incremental metric - latest: {}", latest);

        TokenRecentMetric result = new TokenRecentMetric(latest.getTokenId(), latest.getTimeWindow(), 0,
                latest.getTag());

        // Calculate volume metrics
        result.setVolumeUsd(latest.getVolumeUsd() + newMetric.getVolumeUsd() -
                (slideOut != null ? slideOut.getVolumeUsd() : 0));

        result.setBuyVolumeUsd(latest.getBuyVolumeUsd() + newMetric.getBuyVolumeUsd() -
                (slideOut != null ? slideOut.getBuyVolumeUsd() : 0));

        result.setSellVolumeUsd(latest.getSellVolumeUsd() + newMetric.getSellVolumeUsd() -
                (slideOut != null ? slideOut.getSellVolumeUsd() : 0));

        // Calculate counts
        result.setBuyCount(latest.getBuyCount() + newMetric.getBuyCount() -
                (slideOut != null ? slideOut.getBuyCount() : 0));
        result.setSellCount(latest.getSellCount() + newMetric.getSellCount() -
                (slideOut != null ? slideOut.getSellCount() : 0));
        result.setTxCnt(result.getBuyCount() + result.getSellCount());

        // Calculate buy pressure
        result.setBuyPressureUsd(result.getBuyVolumeUsd() - result.getSellVolumeUsd());

        log.debug("Computed incremental metric: {}", result);
        return result;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TokenRecentMetric> out) throws Exception {
        TimeDomain timeDomain = ctx.timeDomain();
        
        if (timeDomain == TimeDomain.EVENT_TIME) {
            // Output timer - emit current aggregated results
            log.debug("Output timer triggered at timestamp: {} for window: {}", timestamp, windowName);
            
            for (String windowKey : latestMetrics.keys()) {
                TokenRecentMetric metric = latestMetrics.get(windowKey);
                if (metric != null) {
                    // Set the output timestamp 
                    metric.setEndTime(timestamp);
                    out.collect(metric);
                    log.debug("Emitted aggregated metric for window: {}, metric: {}", windowName, metric);
                }
            }
            
        } else if (timeDomain == TimeDomain.PROCESSING_TIME) {
            // Cleanup timer
            log.debug("Cleanup timer triggered at timestamp: {}", timestamp);
            long cutoffTime = timestamp - 3_600_000; // 1小时前
            int cleanupCount = 0;

            // 使用安全的迭代方式避免ConcurrentModificationException
            List<String> keysToRemove = new ArrayList<>();
            for (String key : twentySecMetrics.keys()) {
                // 从键中提取时间戳
                String[] parts = key.split(":");
                if (parts.length > 0) {
                    try {
                        long ts = Long.parseLong(parts[0]);
                        if (ts < cutoffTime) {
                            keysToRemove.add(key);
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Invalid timestamp format in key: {}", key);
                    }
                }
            }
            
            // 批量删除过期的键
            for (String key : keysToRemove) {
                twentySecMetrics.remove(key);
                cleanupCount++;
            }

            if (cleanupCount > 0) {
                log.info("Cleaned up {} old metrics before timestamp {}", cleanupCount, cutoffTime);
            }
        }
    }
}
