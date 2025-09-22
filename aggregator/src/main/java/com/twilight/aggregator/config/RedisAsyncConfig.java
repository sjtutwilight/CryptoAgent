package com.twilight.aggregator.config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class RedisAsyncConfig extends BaseConfig {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(RedisAsyncConfig.class);
    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<String, String> connection;
    private transient RedisAsyncCommands<String, String> asyncCommands;

    private static class SingletonHelper {
        private static final RedisAsyncConfig INSTANCE = new RedisAsyncConfig();
    }

    private RedisAsyncConfig() {
        super();
        initialize();
    }

    public static RedisAsyncConfig getInstance() {
        return SingletonHelper.INSTANCE;
    }

    private void initialize() {
        try {
            String host = getProperty("redis.host", "localhost");
            int port = getIntProperty("redis.port", "6379");
            String password = getProperty("redis.password", "");
            int timeout = getIntProperty("redis.timeout", "2000");

            RedisURI.Builder uriBuilder = RedisURI.builder()
                    .withHost(host)
                    .withPort(port)
                    .withTimeout(Duration.ofMillis(timeout));

            if (!password.isEmpty()) {
                uriBuilder.withPassword(password.toCharArray());
            }

            redisClient = RedisClient.create(uriBuilder.build());
            connection = redisClient.connect();
            asyncCommands = connection.async();
            asyncCommands.setAutoFlushCommands(true);

            log.info("Successfully initialized Redis async connection to {}:{}", host, port);
        } catch (Exception e) {
            log.error("Failed to initialize Redis async connection", e);
            throw new RuntimeException("Failed to initialize Redis async connection", e);
        }
    }

    public RedisAsyncCommands<String, String> getAsyncCommands() {
        return asyncCommands;
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
            log.info("Successfully closed Redis async connection");
        } catch (Exception e) {
            log.error("Error closing Redis async connection", e);
        }
    }
}