package com.twilight.quality.domain.enums;

/**
 * 业务域枚举
 * 定义数据质量引擎支持的业务数据域
 */
public enum DataDomain {
    
    // ===== DEX 链上数据 =====
    
    /**
     * DEX Uniswap 交易数据
     */
    DEX_UNISWAP("dex.uniswap", "dex_transaction", "DEX Uniswap交易"),
    
    /**
     * DEX Hyperliquid 交易数据
     */
    DEX_HYPERLIQUID("dex.hyperliquid", "dex_transaction", "DEX Hyperliquid交易"),
    
    // ===== CEX K线数据 =====
    
    /**
     * CEX K线数据 (Binance等)
     */
    CEX_KLINE("cex.kline", "binance.kline", "CEX K线数据"),
    
    // ===== CEX 永续合约数据 =====
    
    /**
     * 永续合约订单簿
     */
    CEX_PERP_ORDERBOOK("cex.perp.orderbook", "perp.orderbook", "永续合约订单簿"),
    
    /**
     * 永续合约成交数据
     */
    CEX_PERP_TRADES("cex.perp.trades", "perp.trades", "永续合约成交"),
    
    /**
     * 永续合约资金费率
     */
    CEX_PERP_FUNDING("cex.perp.funding", "perp.funding_rate", "永续合约资金费率"),
    
    /**
     * 永续合约持仓量
     */
    CEX_PERP_OPEN_INTEREST("cex.perp.oi", "perp.open_interest", "永续合约持仓量"),
    
    /**
     * 永续合约标记价格
     */
    CEX_PERP_MARK_INDEX("cex.perp.mark", "perp.mark_index", "永续合约标记价格");
    
    private final String domainId;
    private final String kafkaTopic;
    private final String description;
    
    DataDomain(String domainId, String kafkaTopic, String description) {
        this.domainId = domainId;
        this.kafkaTopic = kafkaTopic;
        this.description = description;
    }
    
    public String getDomainId() {
        return domainId;
    }
    
    public String getKafkaTopic() {
        return kafkaTopic;
    }
    
    public String getDescription() {
        return description;
    }
    
    /**
     * 判断是否为DEX域
     */
    public boolean isDex() {
        return this.name().startsWith("DEX_");
    }
    
    /**
     * 判断是否为CEX K线域
     */
    public boolean isKline() {
        return this == CEX_KLINE;
    }
    
    /**
     * 判断是否为CEX永续合约域
     */
    public boolean isPerp() {
        return this.name().startsWith("CEX_PERP_");
    }
    
    /**
     * 根据Kafka topic查找对应的业务域
     */
    public static DataDomain fromKafkaTopic(String topic) {
        for (DataDomain domain : values()) {
            if (domain.kafkaTopic.equals(topic)) {
                return domain;
            }
        }
        return null;
    }
    
    /**
     * 根据domainId查找对应的业务域
     */
    public static DataDomain fromDomainId(String domainId) {
        for (DataDomain domain : values()) {
            if (domain.domainId.equals(domainId)) {
                return domain;
            }
        }
        return null;
    }
}

