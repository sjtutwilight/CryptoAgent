package com.twilight.realtime.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Basic configuration loader for realtime pipeline jobs.
 *
 * <p>The loader looks for an external file specified via the environment
 * variable {@code REALTIME_PIPELINE_CONFIG}. If not provided, it falls back to
 * {@code application.properties} from the classpath.</p>
 */
public class PipelineConfig {
    private static final String CONFIG_ENV = "REALTIME_PIPELINE_CONFIG";
    private final Properties props = new Properties();

    private PipelineConfig() {
    }

    public static PipelineConfig load() {
        PipelineConfig config = new PipelineConfig();
        config.loadInternal();
        return config;
    }

    private void loadInternal() {
        String externalPath = System.getenv(CONFIG_ENV);
        if (externalPath != null && !externalPath.isEmpty()) {
            Path path = Path.of(externalPath);
            if (Files.exists(path)) {
                try (InputStream in = new FileInputStream(path.toFile())) {
                    props.load(in);
                    return;
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to load config from " + path, e);
                }
            }
        }

        try (InputStream in = PipelineConfig.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in == null) {
                throw new IllegalStateException("application.properties not found on classpath");
            }
            props.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load application.properties", e);
        }
    }

    public String getKafkaBootstrapServers() {
        return props.getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    public String getKafkaGroupId() {
        return props.getProperty("kafka.group.id", "realtime-pipeline");
    }

    public String getTopic(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    public String getRedisHost() {
        return props.getProperty("redis.host", "localhost");
    }

    public int getRedisPort() {
        return Integer.parseInt(props.getProperty("redis.port", "6379"));
    }

    public int getRedisDatabase() {
        return Integer.parseInt(props.getProperty("redis.database", "0"));
    }

    public String getRedisPassword() {
        return props.getProperty("redis.password", "");
    }

    public int getRedisTimeoutMs() {
        return Integer.parseInt(props.getProperty("redis.timeout.ms", "2000"));
    }

    public long getRedisCacheMaxSize() {
        return Long.parseLong(props.getProperty("redis.cache.max.size", "200000"));
    }

    public long getRedisCacheTtlSeconds() {
        return Long.parseLong(props.getProperty("redis.cache.ttl.seconds", "300"));
    }

    public int getParallelism() {
        return Integer.parseInt(props.getProperty("pipeline.parallelism", "4"));
    }

    public long getCheckpointIntervalMs() {
        return Long.parseLong(props.getProperty("pipeline.checkpoint.interval.ms", "60000"));
    }

    public long getCheckpointMinPauseMs() {
        return Long.parseLong(props.getProperty("pipeline.min.pause.between.checkpoints.ms", "30000"));
    }

    public long getMaxOutOfOrdernessSeconds() {
        return Long.parseLong(props.getProperty("pipeline.watermark.max.out_of_orderness.seconds", "15"));
    }

    public long getPriceLookupMaxPastMillis() {
        long seconds = Long.parseLong(props.getProperty("price.lookup.max.age.seconds", "30"));
        return Math.max(seconds, 1L) * 1000L;
    }

    public int getChainId() {
        return Integer.parseInt(props.getProperty("pipeline.chain.id", "1"));
    }

    public String getNativeTokenAddress() {
        return props.getProperty("pipeline.native.token.address", "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
    }
}
