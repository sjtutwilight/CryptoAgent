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
 * DEX交易数据消费者
 * 订阅 dex_transaction topic
 */
@Component
public class DexTransactionConsumer extends BaseQualityConsumer {
    
    public DexTransactionConsumer(RuleEngine ruleEngine,
                                   AlertDispatcher alertDispatcher,
                                   QualityMetricSink metricSink) {
        super(ruleEngine, alertDispatcher, metricSink);
    }
    
    /**
     * 消费DEX交易消息
     */
    @KafkaListener(
            topics = "dex_transaction",
            groupId = "quality-engine-dex",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<String> messages) {
        log.debug("收到 {} 条DEX交易消息", messages.size());
        
        // 根据消息内容判断具体的DEX类型
        for (String message : messages) {
            DataDomain domain = detectDexDomain(message);
            processMessage(message, domain);
        }
    }
    
    /**
     * 检测DEX类型
     */
    private DataDomain detectDexDomain(String message) {
        // 默认使用Uniswap域，实际可根据消息内容判断
        // 例如通过chain_id或其他标识
        try {
            JsonNode node = objectMapper.readTree(message);
            String chainId = node.has("chain_id") ? node.get("chain_id").asText() : null;
            
            if ("hyperliquid".equalsIgnoreCase(chainId)) {
                return DataDomain.DEX_HYPERLIQUID;
            }
        } catch (Exception e) {
            // 忽略解析错误，使用默认值
        }
        
        return DataDomain.DEX_UNISWAP;
    }
    
    @Override
    protected String extractStreamKey(JsonNode message, DataDomain domain) {
        // DEX交易使用 chain_id 作为流标识
        if (message.has("transaction") && message.get("transaction").has("chain_id")) {
            return message.get("transaction").get("chain_id").asText();
        }
        if (message.has("chain_id")) {
            return message.get("chain_id").asText();
        }
        return "unknown";
    }
    
    @Override
    protected Long extractEventTime(JsonNode message, DataDomain domain) {
        // 尝试从多个可能的字段提取时间戳
        if (message.has("transaction") && message.get("transaction").has("timestamp")) {
            return message.get("transaction").get("timestamp").asLong();
        }
        if (message.has("timestamp")) {
            return message.get("timestamp").asLong();
        }
        if (message.has("block_time")) {
            return message.get("block_time").asLong();
        }
        return System.currentTimeMillis();
    }
}

