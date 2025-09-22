package com.twilight.aggregator.process.token;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.Token;
import com.twilight.aggregator.model.TokenRecentMetric;

import java.util.HashMap;
import java.util.Map;

public class TokenSlidingWindowProcessor extends ProcessWindowFunction<Token, TokenRecentMetric, String, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private final String windowName;
    private transient Logger log;

    // State to store 20s window metrics
    private transient MapState<Long, TokenRecentMetric> twentySecMetrics;

    public TokenSlidingWindowProcessor(String windowName) {
        this.windowName = windowName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.log = LoggerFactory.getLogger(TokenSlidingWindowProcessor.class);

        // Initialize state for 20s metrics
        MapStateDescriptor<Long, TokenRecentMetric> twentySecMetricsDescriptor = new MapStateDescriptor<>(
                "twentySecMetrics",
                Long.class, TokenRecentMetric.class);
        this.twentySecMetrics = getRuntimeContext().getMapState(twentySecMetricsDescriptor);
    }

    @Override
    public void process(String key, Context context, Iterable<Token> elements, Collector<TokenRecentMetric> out) {
        // 创建一个 Map 来存储不同 tag 的指标
        Map<String, TokenRecentMetric> tagMetrics = new HashMap<>();
        log.info("Processing TokenRecentMetric for key: {}", key);
        // 创建一个默认的 "all" 指标
        TokenRecentMetric allMetric = new TokenRecentMetric(null, windowName, context.window().getEnd(), "all");
        tagMetrics.put("all", allMetric);

        for (Token token : elements) {
            // 获取 token 的 tag
            String tag = token.getFromAddressTag();
            if (tag == null || tag.isEmpty()) {
                tag = "all";
            }

            // 获取或创建对应 tag 的指标
            TokenRecentMetric metric = tagMetrics.get(tag);
            if (metric == null) {
                metric = new TokenRecentMetric(null, windowName, context.window().getEnd(), tag);
                tagMetrics.put(tag, metric);
            }

            // 更新 tag 指标
            updateMetric(metric, token);

            // 同时更新 "all" 指标
            if (!tag.equals("all")) {
                updateMetric(allMetric, token);
            }
        }

        try {
            // 输出所有 tag 的指标
            for (TokenRecentMetric metric : tagMetrics.values()) {
                // 存储指标到状态
                twentySecMetrics.put(context.window().getEnd(), metric);
                log.debug("Stored metric in state: key={}, tag={}, metric={}",
                        context.window().getEnd(), metric.getTag(), metric);
                out.collect(metric);
            }
        } catch (Exception e) {
            log.error("Error processing window for token {}: {}", key, e.getMessage());
        }
    }

    private void updateMetric(TokenRecentMetric metric, Token token) {
        metric.setTokenId(token.getTokenId());
        double volumeUsd = token.getAmount() * token.getTokenPriceUsd();
        metric.setVolumeUsd(metric.getVolumeUsd() + volumeUsd);
        metric.setTokenPriceUsd(token.getTokenPriceUsd()); // 使用最新价格
        metric.setTxCnt(metric.getTxCnt() + 1);
        if (token.isBuyOrSell()) {
            metric.setBuyVolumeUsd(metric.getBuyVolumeUsd() + volumeUsd);
            metric.setBuyCount(metric.getBuyCount() + 1);
        } else {
            metric.setSellVolumeUsd(metric.getSellVolumeUsd() + volumeUsd);
            metric.setSellCount(metric.getSellCount() + 1);
        }
        metric.setBuyPressureUsd(metric.getBuyVolumeUsd() - metric.getSellVolumeUsd());
        
        // 设置Token指标信息（使用最新的指标值）
        if (token.getMcapUsd() != null) {
            metric.setMcapUsd(token.getMcapUsd());
        }
        if (token.getFdvUsd() != null) {
            metric.setFdvUsd(token.getFdvUsd());
        }
        if (token.getLiquidityUsd() != null) {
            metric.setLiquidityUsd(token.getLiquidityUsd());
        }
    }
}