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
 * K线数据消费者
 * 订阅 binance.kline topic
 */
@Component
public class KlineConsumer extends BaseQualityConsumer {
    
    public KlineConsumer(RuleEngine ruleEngine,
                          AlertDispatcher alertDispatcher,
                          QualityMetricSink metricSink) {
        super(ruleEngine, alertDispatcher, metricSink);
    }
    
    /**
     * 消费K线消息
     */
    @KafkaListener(
            topics = "binance.kline",
            groupId = "quality-engine-kline",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<String> messages) {
        log.debug("收到 {} 条K线消息", messages.size());
        processMessages(messages, DataDomain.CEX_KLINE);
    }
    
    @Override
    protected String extractStreamKey(JsonNode message, DataDomain domain) {
        // K线使用 symbol + interval 作为流标识
        String symbol = message.has("symbol") ? message.get("symbol").asText() : "unknown";
        String interval = message.has("interval") ? message.get("interval").asText() : "1m";
        return symbol + "_" + interval;
    }
    
    @Override
    protected Long extractEventTime(JsonNode message, DataDomain domain) {
        // K线事件时间
        if (message.has("close_time")) {
            return message.get("close_time").asLong();
        }
        if (message.has("event_time")) {
            return message.get("event_time").asLong();
        }
        if (message.has("k") && message.get("k").has("T")) {
            return message.get("k").get("T").asLong();
        }
        return System.currentTimeMillis();
    }
}

