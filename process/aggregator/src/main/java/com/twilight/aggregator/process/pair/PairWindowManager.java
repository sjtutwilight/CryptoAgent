package com.twilight.aggregator.process.pair;

import java.io.Serializable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.twilight.aggregator.model.Pair;
import com.twilight.aggregator.model.PairMetric;

/**
 * Manages the hierarchical window processing for pair metrics.
 * This class encapsulates the logic for creating and connecting multiple window
 * levels
 * to process pair data at different time granularities.
 */
public class PairWindowManager {

        /**
         * Creates a hierarchical window processing pipeline for pair metrics.
         * The pipeline consists of:
         * 1. Base window (20s)
         * 2. 1-minute window aggregated from 20s windows
         * 3. 5-minute window aggregated from 1-minute windows
         * 4. 30-minute window aggregated from 5-minute windows
         *
         * @param pairStream The input pair stream to process
         * @return A unified stream containing metrics from all window levels
         */
        public static DataStream<PairMetric> createHierarchicalWindows(KeyedStream<Pair, Long> pairStream) {
                // Base window (20s)
                DataStream<PairMetric> baseWindow = pairStream
                                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                                .allowedLateness(Time.seconds(20))
                                .process(new PairWindowProcessor("20s"))
                                .name("20s-pair-window");

                // 1-minute window from 20s windows
                DataStream<PairMetric> oneMinWindow = baseWindow
                                .keyBy(metric -> metric.getPairId().toString())
                                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                                .allowedLateness(Time.seconds(30))
                                .process(new PairHierarchicalWindowAggregator("1min"))
                                .name("1min-pair-window");

                // 5-minute window from 1-minute windows
                DataStream<PairMetric> fiveMinWindow = oneMinWindow
                                .keyBy(metric -> metric.getPairId().toString())
                                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                                .allowedLateness(Time.minutes(1))
                                .process(new PairHierarchicalWindowAggregator("5min"))
                                .name("5min-pair-window");

                // 1-hour window from 5-minute windows
                DataStream<PairMetric> oneHourWindow = fiveMinWindow
                                .keyBy(metric -> metric.getPairId().toString())
                                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                                .allowedLateness(Time.minutes(5))
                                .process(new PairHierarchicalWindowAggregator("1h"))
                                .name("1h-pair-window");

                // Merge all window results
                return baseWindow
                                .union(oneMinWindow)
                                .union(fiveMinWindow)
                                .union(oneHourWindow);
        }
}