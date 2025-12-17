package com.twilight.quality;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 数据质量引擎启动类
 * 
 * 功能：
 * 1. 实时检测：每条消息的完整性、准确性、模式校验
 * 2. 微批检测：窗口聚合的时效性、吞吐量、一致性检测
 * 3. 告警分发：Kafka事件 + Webhook通知
 * 
 * 支持业务域：
 * - DEX: Uniswap, Hyperliquid
 * - CEX: Kline, 永续合约(orderbook/trades/funding)
 */
@SpringBootApplication
@EnableScheduling
public class QualityEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(QualityEngineApplication.class, args);
    }
}

