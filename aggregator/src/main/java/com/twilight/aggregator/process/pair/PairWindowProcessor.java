package com.twilight.aggregator.process.pair;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.RedisAsyncConfig;
import com.twilight.aggregator.model.Pair;
import com.twilight.aggregator.model.PairMetric;
import com.twilight.aggregator.utils.EthereumUtils;

import io.lettuce.core.api.async.RedisAsyncCommands;

public class PairWindowProcessor extends ProcessWindowFunction<Pair, PairMetric, Long, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private final String windowName;
    private transient Logger log;
    private String usdcAddress;
    private transient RedisAsyncConfig redisConfig;
    private transient RedisAsyncCommands<String, String> asyncCommands;

    public PairWindowProcessor(String windowName) {
        this.windowName = windowName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.log = LoggerFactory.getLogger(PairWindowProcessor.class);
        this.usdcAddress = "0x74a6379d012ce53e3b0718c05dd72a3de87f0c6a";
        this.redisConfig = RedisAsyncConfig.getInstance();
        this.asyncCommands = redisConfig.getAsyncCommands();
        log.info("Initialized PairWindowProcessor with Redis async connection");
    }

    @Override
    public void close() throws Exception {
        // Redis connection is managed by RedisAsyncConfig singleton
        super.close();
    }

    @Override
    public void process(Long key, Context context, Iterable<Pair> elements, Collector<PairMetric> out) {
        log.info("Processing pair metrics for window: {}", windowName);
        PairMetric metric = new PairMetric();
        metric.setTimeWindow(windowName);
        metric.setEndTime(context.window().getEnd());

        Pair latestSyncEvent = null;

        for (Pair pair : elements) {
            updateMetric(metric, pair);
            metric.setPairId(pair.getPairId());

            if ("Sync".equals(pair.getEventName())) {
                latestSyncEvent = pair;
            }
        }

        if (latestSyncEvent != null) {
            updateTokenPricesInRedis(latestSyncEvent)
                    .thenRun(() -> out.collect(metric))
                    .exceptionally(throwable -> {
                        log.error("Error updating token prices: {}", throwable.getMessage());
                        out.collect(metric);
                        return null;
                    });
        } else {
            out.collect(metric);
        }
    }

    private CompletableFuture<Void> updateTokenPricesInRedis(Pair pair) {
        String token0Address = pair.getToken0Address().toLowerCase();
        String token1Address = pair.getToken1Address().toLowerCase();

        if (!token0Address.equals(usdcAddress) && !token1Address.equals(usdcAddress)) {
            return CompletableFuture.completedFuture(null);
        }

        double token0Reserve = pair.getReserve0();
        double token1Reserve = pair.getReserve1();

        CompletableFuture<Void> token0Future = CompletableFuture.runAsync(() -> {
            if (token1Reserve > 0 && token0Address.equals(usdcAddress)) {
                double tokenPrice = token0Reserve / token1Reserve;
                asyncCommands.set("token_price:" + token1Address, String.valueOf(tokenPrice))
                        .thenAccept(
                                result -> log.debug("Updated price for token {}: {} USD", token1Address, tokenPrice))
                        .exceptionally(throwable -> {
                            log.error("Failed to update token1 price in Redis: {}", throwable.getMessage());
                            return null;
                        });
            }
        });

        CompletableFuture<Void> token1Future = CompletableFuture.runAsync(() -> {
            if (token0Reserve > 0 && token1Address.equals(usdcAddress)) {
                double tokenPrice = token1Reserve / token0Reserve;
                asyncCommands.set("token_price:" + token0Address, String.valueOf(tokenPrice))
                        .thenAccept(
                                result -> log.debug("Updated price for token {}: {} USD", token0Address, tokenPrice))
                        .exceptionally(throwable -> {
                            log.error("Failed to update token0 price in Redis: {}", throwable.getMessage());
                            return null;
                        });
            }
        });

        return CompletableFuture.allOf(token0Future, token1Future)
                .orTimeout(2, TimeUnit.SECONDS)
                .exceptionally(throwable -> {
                    log.error("Token price update timed out: {}", throwable.getMessage());
                    return null;
                });
    }

    private void updateMetric(PairMetric metric, Pair pair) {
        switch (pair.getEventName()) {
            case "Sync":
                metric.setToken0Reserve(pair.getReserve0());
                metric.setToken1Reserve(pair.getReserve1());
                metric.setReserveUsd(calculateReserveUsd(pair, pair.getReserve0(), pair.getReserve1()));
                metric.incrementTxCount();
                break;
            case "Swap":
                updateSwapMetrics(metric, pair);
                metric.incrementTxCount();
                break;
            case "Mint":
            case "Burn":
                metric.incrementTxCount();
                break;
        }
    }

    private double calculateReserveUsd(Pair pair, double token0Reserve, double token1Reserve) {
        return token0Reserve * pair.getToken0PriceUsd() + token1Reserve * pair.getToken1PriceUsd();
    }

    private void updateSwapMetrics(PairMetric metric, Pair pair) {
        double amount0Total = pair.getAmount0In() + pair.getAmount0Out();
        double amount1Total = pair.getAmount1In() + pair.getAmount1Out();

        double volume0Usd = amount0Total * pair.getToken0PriceUsd();
        double volume1Usd = amount1Total * pair.getToken1PriceUsd();

        metric.setToken0VolumeUsd(metric.getToken0VolumeUsd() + volume0Usd);
        metric.setToken1VolumeUsd(metric.getToken1VolumeUsd() + volume1Usd);
        metric.setVolumeUsd(metric.getVolumeUsd() + volume0Usd + volume1Usd);
    }
}