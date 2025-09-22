package com.twilight.aggregator.config;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;

public class FlinkConfig extends BaseConfig {

    private static class SingletonHelper {
        private static final FlinkConfig INSTANCE = new FlinkConfig();
    }

    private FlinkConfig() {
        super();
    }

    public static FlinkConfig getInstance() {
        return SingletonHelper.INSTANCE;
    }

    // Flink基本配置
    public int getParallelism() {
        return getIntProperty("flink.parallelism", "1");
    }

    public long getCheckpointInterval() {
        return getLongProperty("flink.checkpoint.interval", "60000");
    }

    // Kafka Source配置
    public String getKafkaBootstrapServers() {
        return getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    public String getKafkaTopic() {
        return getProperty("kafka.topic", "dex_transaction");
    }

    public String getKafkaGroupId() {
        return getProperty("kafka.group.id", "flink-aggregator");
    }

    // 元数据配置
    public long getPairMetadataRefreshInterval() {
        return getLongProperty("pair.metadata.refresh.interval", "60000");
    }

    public String getUsdcAddress() {
        return getProperty("token.usdc.address", "0x74A6379d012ce53E3b0718C05dD72a3De87F0c6a");
    }

    // 价格刷新间隔配置
    public long getPriceRefreshInterval() {
        return getLongProperty("price.refresh.interval", "10000"); // 10秒刷新一次价格
    }

    // ClickHouse配置方法
    public String getClickHouseUrl() {
        return getProperty("clickhouse.url");
    }

    public String getClickHouseUsername() {
        return getProperty("clickhouse.username", "default");
    }

    public String getClickHousePassword() {
        return getProperty("clickhouse.password", "");
    }

    public String getClickHouseDriverClassName() {
        return getProperty("clickhouse.driver.class.name", "com.clickhouse.jdbc.ClickHouseDriver");
    }

    public int getClickHouseBatchSize() {
        return Integer.parseInt(getProperty("clickhouse.batch.size", "1000"));
    }

    public int getClickHouseBatchInterval() {
        return Integer.parseInt(getProperty("clickhouse.batch.interval", "5000"));
    }

    // Environment Configuration
    public Configuration getEnvironmentConfig() {
        Configuration configuration = new Configuration();

        // Web UI配置
        configuration.setString("rest.bind-port", getProperty("flink.rest.bind-port", "8083"));
        configuration.setString("rest.address", getProperty("flink.rest.address", "localhost"));

        // 资源配置
        configuration.setString("taskmanager.memory.network.fraction",
                getProperty("flink.taskmanager.memory.network.fraction", "0.3"));
        configuration.setInteger("taskmanager.numberOfTaskSlots", getIntProperty("flink.taskmanager.slots", "1"));
        configuration.setString("taskmanager.memory.task.heap.size",
                getProperty("flink.taskmanager.memory.task.heap.size", "2048m"));
        configuration.setString("taskmanager.memory.task.off-heap.size",
                getProperty("flink.taskmanager.memory.task.off-heap.size", "512m"));
        configuration.setString("taskmanager.memory.network.min",
                getProperty("flink.taskmanager.memory.network.min", "512mb"));
        configuration.setString("taskmanager.memory.network.max",
                getProperty("flink.taskmanager.memory.network.max", "512mb"));
        configuration.setString("jobmanager.memory.process.size",
                getProperty("flink.jobmanager.memory.process.size", "1024m"));

        // 缓冲区配置
        configuration.setString("taskmanager.network.memory.buffer-size",
                getProperty("flink.taskmanager.network.buffer-size", "32kb"));
        configuration.setString("taskmanager.network.memory.floating-buffers-per-gate",
                getProperty("flink.taskmanager.network.floating-buffers-per-gate", "1024"));
        configuration.setString("taskmanager.network.memory.buffers-per-channel",
                getProperty("flink.taskmanager.network.buffers-per-channel", "8"));
        configuration.setString("taskmanager.network.detailed-metrics",
                getProperty("flink.taskmanager.network.detailed-metrics", "false"));
        configuration.setBoolean("taskmanager.network.blocking-shuffle.compression.enabled",
                getBooleanProperty("flink.taskmanager.network.blocking-shuffle.compression.enabled", "false"));

        // 状态后端配置
        configuration.setString("state.backend", getProperty("flink.state.backend", "hashmap"));
        configuration.setString("state.backend.incremental", getProperty("flink.state.backend.incremental", "true"));
        configuration.setString("state.checkpoints.num-retained",
                getProperty("flink.state.checkpoints.num-retained", "2"));

        // 重启策略配置
        configuration.setString("restart-strategy", getProperty("flink.restart-strategy", "fixed-delay"));
        configuration.setString("restart-strategy.fixed-delay.attempts",
                getProperty("flink.restart-strategy.fixed-delay.attempts", "3"));
        configuration.setString("restart-strategy.fixed-delay.delay",
                getProperty("flink.restart-strategy.fixed-delay.delay", "10s"));

        return configuration;
    }

    // Account Balance相关配置
    public String getKafkaGroupIdBalance() {
        return getProperty("kafka.group.id.balance", "balance-processor-group");
    }

    // Kafka Properties
    public Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("fetch.min.bytes", getProperty("kafka.fetch.min.bytes", "1"));
        properties.setProperty("fetch.max.wait.ms", getProperty("kafka.fetch.max.wait.ms", "100"));
        properties.setProperty("max.poll.records", getProperty("kafka.max.poll.records", "500"));
        properties.setProperty("enable.auto.commit", getProperty("kafka.enable.auto.commit", "false"));
        properties.setProperty("auto.offset.reset", getProperty("kafka.auto.offset.reset", "latest"));
        properties.setProperty("isolation.level", getProperty("kafka.isolation.level", "read_committed"));
        return properties;
    }
}
