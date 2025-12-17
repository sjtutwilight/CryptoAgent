package com.twilight.quality.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.alert.AlertDispatcher;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.rule.RuleEngine;
import com.twilight.quality.sink.QualityMetricSink;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 永续合约数据消费者
 * 订阅多个永续合约相关topic
 */
@Component
public class PerpConsumer extends BaseQualityConsumer {
    
    public PerpConsumer(RuleEngine ruleEngine,
                         AlertDispatcher alertDispatcher,
                         QualityMetricSink metricSink) {
        super(ruleEngine, alertDispatcher, metricSink);
    }
    
    /**
     * 消费订单簿消息
     */
    @KafkaListener(
            topics = "perp.orderbook",
            groupId = "quality-engine-perp-ob",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderbook(List<String> messages) {
        log.debug("收到 {} 条订单簿消息", messages.size());
        processMessages(messages, DataDomain.CEX_PERP_ORDERBOOK);
    }
    
    /**
     * 消费成交消息
     */
    @KafkaListener(
            topics = "perp.trades",
            groupId = "quality-engine-perp-trades",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeTrades(List<String> messages) {
        log.debug("收到 {} 条成交消息", messages.size());
        processMessages(messages, DataDomain.CEX_PERP_TRADES);
    }
    
    /**
     * 消费资金费率消息
     */
    @KafkaListener(
            topics = "perp.funding_rate",
            groupId = "quality-engine-perp-funding",
            containerFactory = "singleMessageListenerFactory"
    )
    public void consumeFunding(String message) {
        log.debug("收到资金费率消息");
        processMessage(message, DataDomain.CEX_PERP_FUNDING);
    }
    
    /**
     * 消费持仓量消息
     */
    @KafkaListener(
            topics = "perp.open_interest",
            groupId = "quality-engine-perp-oi",
            containerFactory = "singleMessageListenerFactory"
    )
    public void consumeOpenInterest(String message) {
        log.debug("收到持仓量消息");
        processMessage(message, DataDomain.CEX_PERP_OPEN_INTEREST);
    }
    
    /**
     * 消费标记价格消息
     */
    @KafkaListener(
            topics = "perp.mark_index",
            groupId = "quality-engine-perp-mark",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMarkIndex(List<String> messages) {
        log.debug("收到 {} 条标记价格消息", messages.size());
        processMessages(messages, DataDomain.CEX_PERP_MARK_INDEX);
    }
    
    @Override
    protected String extractStreamKey(JsonNode message, DataDomain domain) {
        // 永续合约使用 symbol + exchange 作为流标识
        String symbol = message.has("symbol") ? message.get("symbol").asText() : "unknown";
        String exchange = message.has("exchange") ? message.get("exchange").asText() : "binance";
        return symbol + "_" + exchange;
    }
    
    @Override
    protected Long extractEventTime(JsonNode message, DataDomain domain) {
        // 尝试从多个可能的字段提取时间戳
        if (message.has("event_time")) {
            return message.get("event_time").asLong();
        }
        if (message.has("timestamp")) {
            return message.get("timestamp").asLong();
        }
        if (message.has("E")) {
            return message.get("E").asLong();
        }
        if (message.has("time")) {
            return message.get("time").asLong();
        }
        return System.currentTimeMillis();
    }
}

