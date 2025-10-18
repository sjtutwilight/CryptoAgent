package com.crypto.control.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "sequence-detector")
public class SequenceDetectorProperties {

    private DetectorConfig detector = new DetectorConfig();
    private Map<String, SourceConfig> sources = new HashMap<>();

    @Data
    public static class DetectorConfig {
        /**
         * 是否开启序列缺失检测。
         */
        private boolean enabled = true;
        /**
         * 单次缺失允许的最大跨度（超过则拆分多个任务）。
         */
        private int maxGapRange = 50;
        /**
         * 任务分片大小（一次回补多少序列）。
         */
        private int batchSize = 20;
        /**
         * 状态在 Redis 中的 TTL(秒)。
         */
        private long stateTtlSeconds = 86400;
    }

    @Data
    public static class SourceConfig {
        /**
         * 序列类型，对应 SequenceExtractJob 输出中的 type。
         */
        private String type;
        /**
         * 数据源唯一标识，供限流&任务调度使用。
         */
        private String dataSourceId;
        /**
         * worker 将执行的任务类型，默认 http_jsonrpc。
         */
        private String taskType = "http_jsonrpc";
        /**
         * 任务调用的方法名，如 eth_getBlockRange。
         */
        private String method;
        /**
         * 可选：HTTP endpoint 或路径信息。
         */
        private String url;
        /**
         * 单个缺失任务的最大跨度。
         */
        private Integer maxGapRange;
        /**
         * 任务切片大小。
         */
        private Integer batchSize;
        /**
         * 调用成本，默认 1。
         */
        private Integer cost = 1;
        /**
         * 调用优先级，默认 5。
         */
        private Integer priority = 5;
        /**
         * 是否启用当前源的缺失检测。
         */
        private boolean enabled = true;
    }

    public SourceConfig getSourceConfig(String type) {
        return sources.get(type);
    }
}
