package com.crypto.control.config;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "datasources")
public class DataSourceConfigProperties {

    /**
     * key = dataSourceId (如 mock-ethereum, coinmarketcap, binance-websocket)
     */
    private Map<String, DataSourceConfig> configs;

    @Data
    public static class DataSourceConfig {
        private String dataSourceId;
        private String baseUrl;
        private Integer rateLimitWeight = 60;
        private Integer rateLimitInterval = 60;
        private Integer maxRetryCount = 3;
        private Integer retryDelayMs = 1000;
        private Integer timeoutMs = 30000;
        private Boolean enabled = true;
    }

    public DataSourceConfig getDataSourceConfig(String dataSourceId) {
        return configs.get(dataSourceId);
    }
}