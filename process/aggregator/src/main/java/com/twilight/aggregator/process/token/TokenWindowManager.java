package com.twilight.aggregator.process.token;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;

import com.twilight.aggregator.model.Token;
import com.twilight.aggregator.model.TokenRecentMetric;
// import com.twilight.aggregator.process.token.TokenRollingWindowAggregator; // 暂时注释掉
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the hierarchical window processing for token metrics.
 * This class encapsulates the logic for creating and connecting multiple window
 * levels
 * to process token data at different time granularities.
 */
public class TokenWindowManager {
        private static final Logger log = LoggerFactory.getLogger(TokenWindowManager.class);

        /**
         * Creates a hierarchical window processing pipeline for token metrics.
         * The pipeline consists of:
         * 1. Base window (20s)
         * 2. 1-minute window aggregated from 20s windows
         * 3. 5-minute window aggregated from 1-minute windows
         * 4. 30-minute window aggregated from 5-minute windows
         *
         * @param tokenStream The input token stream to process
         * @return A unified stream containing metrics from all window levels
         */
        // 暂时注释掉createRollingHierarchicalWindows方法，因为缺少TokenRollingWindowAggregator
        /*
        public static DataStream<TokenRollingMetric> createRollingHierarchicalWindows(
                        KeyedStream<Token, String> tokenStream) {
                log.debug("Creating rolling hierarchical windows for token stream");

                // 添加一个map操作，记录每个Token的时间戳
                KeyedStream<Token, String> loggedTokenStream = tokenStream
                                .map(token -> {
                                        return token;
                                })
                                .keyBy(Token::getTokenAddress);

                // Base window (20s)
                DataStream<TokenRollingMetric> baseWindow = loggedTokenStream
                                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                                .allowedLateness(Time.seconds(2)) // 减少到2秒
                                .trigger(EventTimeTrigger.create())
                                .aggregate(new TokenRollingWindowAggregator())
                                .map(metric -> {
                                        if (metric.getTimeWindow() == null) {
                                                metric.setTimeWindow("20s");
                                                log.debug("Setting timeWindow to 20s for metric: {}", metric);
                                        }

                                        if (metric.getTokenId() == null) {
                                                log.error("Base window metric has null tokenId: {}", metric);
                                        }

                                        return metric;
                                })
                                .name("20s-token-rolling-window");

                // 1-minute window from 20s windows
                DataStream<TokenRollingMetric> oneMinWindow = baseWindow
                                .keyBy(metric -> {
                                        if (metric.getTokenId() == null) {
                                                log.error("TokenId is null in 1-minute window keyBy operation");
                                                return "unknown";
                                        }
                                        return metric.getTokenId().toString();
                                })
                                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                                .allowedLateness(Time.seconds(20))
                                .process(new TokenRollingHierarchicalWindowAggregator("1min"))
                                .name("1min-token-rolling-window");

                // 5-minute window from 1-minute windows
                DataStream<TokenRollingMetric> fiveMinWindow = oneMinWindow
                                .keyBy(metric -> {
                                        if (metric.getTokenId() == null) {
                                                log.error("TokenId is null in 5-minute window keyBy operation");
                                                return "unknown";
                                        }
                                        return metric.getTokenId().toString();
                                })
                                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                                .allowedLateness(Time.seconds(30))
                                .process(new TokenRollingHierarchicalWindowAggregator("5min"))
                                .name("5min-token-rolling-window");

                // 1h window from 5-minute windows
                DataStream<TokenRollingMetric> thirtyMinWindow = fiveMinWindow
                                .keyBy(metric -> {
                                        if (metric.getTokenId() == null) {
                                                log.error("TokenId is null in 1h window keyBy operation");
                                                return "unknown";
                                        }
                                        return metric.getTokenId().toString();
                                })
                                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                                .allowedLateness(Time.minutes(1))
                                .process(new TokenRollingHierarchicalWindowAggregator("1h"))
                                .name("1h-token-rolling-window");

                log.debug("Completed creating rolling hierarchical windows for token stream");
                return baseWindow
                                .union(oneMinWindow)
                                .union(fiveMinWindow)
                                .union(thirtyMinWindow);
        }
        */

        public static DataStream<TokenRecentMetric> createSlidingHierarchicalWindows(
                        KeyedStream<Token, String> tokenStream) {
                log.debug("Creating sliding hierarchical windows for token stream");

                // Base window (20s)
                DataStream<TokenRecentMetric> baseWindow = tokenStream
                                .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(20)))
                                .allowedLateness(Time.seconds(10))
                                .process(new TokenSlidingWindowProcessor("20s"))
                                .name("20s-token-window");

                DataStream<TokenRecentMetric> oneMinWindow = baseWindow
                                .keyBy(metric -> {
                                        if (metric.getTokenId() == null) {
                                                log.error("TokenId is null in 1-minute sliding window keyBy operation");
                                                return "unknown";
                                        }
                                        return metric.getTokenId().toString();
                                })
                                .process(new TokenSlidingHierarchicalAggregator("1min"))
                                .name("1min-token-window");

                DataStream<TokenRecentMetric> fiveMinWindow = oneMinWindow
                                .keyBy(metric -> {
                                        if (metric.getTokenId() == null) {
                                                log.error("TokenId is null in 5-minute sliding window keyBy operation");
                                                return "unknown";
                                        }
                                        return metric.getTokenId().toString();
                                })
                                .process(new TokenSlidingHierarchicalAggregator("5min"))
                                .name("5min-token-window");

                DataStream<TokenRecentMetric> thirtyMinWindow = fiveMinWindow
                                .keyBy(metric -> {
                                        if (metric.getTokenId() == null) {
                                                log.error("TokenId is null in 1h sliding window keyBy operation");
                                                return "unknown";
                                        }
                                        return metric.getTokenId().toString();
                                })
                                .process(new TokenSlidingHierarchicalAggregator("1h"))
                                .name("1h-token-window");

                log.debug("Completed creating sliding hierarchical windows for token stream");
                return baseWindow
                                .union(oneMinWindow)
                                .union(fiveMinWindow)
                                .union(thirtyMinWindow);
        }

}