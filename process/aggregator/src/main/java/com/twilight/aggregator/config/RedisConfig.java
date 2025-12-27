package com.twilight.aggregator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConfig extends BaseConfig {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = LoggerFactory.getLogger(RedisConfig.class);
    private volatile JedisPool jedisPool;

    // 存储配置信息
    private final String host;
    private final int port;
    private final String password;
    private final int maxTotal;
    private final int maxIdle;
    private final int minIdle;
    private final long maxWaitMillis;

    private RedisConfig() {
        super();
        // 在构造函数中初始化配置
        this.host = getProperty("redis.host", "localhost");
        this.port = getIntProperty("redis.port", "6379");
        this.password = getProperty("redis.password", "");
        this.maxTotal = getIntProperty("redis.pool.maxTotal", "50");
        this.maxIdle = getIntProperty("redis.pool.maxIdle", "10");
        this.minIdle = getIntProperty("redis.pool.minIdle", "5");
        this.maxWaitMillis = getLongProperty("redis.pool.maxWaitMillis", "5000");
    }

    private static class SingletonHelper {
        private static final RedisConfig INSTANCE = new RedisConfig();
    }

    public static RedisConfig getInstance() {
        return SingletonHelper.INSTANCE;
    }

    private JedisPool getJedisPool() {
        if (jedisPool == null) {
            synchronized (this) {
                if (jedisPool == null) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(maxTotal);
                    config.setMaxIdle(maxIdle);
                    config.setMinIdle(minIdle);
                    config.setMaxWaitMillis(maxWaitMillis);
                    config.setTestWhileIdle(true);
                    jedisPool = (password.isEmpty()) ? new JedisPool(config, host, port, 5000)
                            : new JedisPool(config, host, port, 5000, password);
                    LOG.info("Redis connection pool initialized with host: {}, port: {}", host, port);
                }
            }
        }
        return jedisPool;
    }

    public Jedis getResource() {
        return getJedisPool().getResource();
    }

    public synchronized void close() {
        if (jedisPool != null) {
            jedisPool.close();
            jedisPool = null;
        }
    }
}
