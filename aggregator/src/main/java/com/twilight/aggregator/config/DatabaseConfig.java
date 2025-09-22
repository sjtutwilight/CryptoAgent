package com.twilight.aggregator.config;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DatabaseConfig extends BaseConfig {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseConfig.class);
    private static transient DatabaseConfig instance;
    private transient HikariDataSource hikariDataSource;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;

    private DatabaseConfig() {
        super();
    }

    private static class SingletonHelper {
        private static final DatabaseConfig INSTANCE = new DatabaseConfig();
    }

    public static DatabaseConfig getInstance() {
        return SingletonHelper.INSTANCE;
    }

    // JDBC基本配置
    public String getJdbcUrl() {
        String url = getProperty("jdbc.url");
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("JDBC URL 配置为空，请检查 application-dev.properties");
        }
        return url;

    }

    public String getJdbcUsername() {
        return getProperty("jdbc.username");
    }

    public String getJdbcPassword() {
        return getProperty("jdbc.password");
    }

    public String getJdbcDriverClassName() {
        return getProperty("jdbc.driver.class.name", "org.postgresql.Driver");
    }

    // 连接池配置
    public int getJdbcMaxPoolSize() {
        return getIntProperty("jdbc.pool.max.size", "10");
    }

    public int getJdbcMinPoolSize() {
        return getIntProperty("jdbc.pool.min.size", "5");
    }

    public long getJdbcConnectionTimeout() {
        return getLongProperty("jdbc.pool.connection.timeout", "30000");
    }

    public long getJdbcIdleTimeout() {
        return getLongProperty("jdbc.pool.idle.timeout", "600000");
    }

    public long getJdbcMaxLifetime() {
        return getLongProperty("jdbc.pool.max.lifetime", "1800000");
    }

    // JDBC Sink配置
    public int getJdbcBatchSize() {
        return getIntProperty("jdbc.batch.size", "1000");
    }

    public int getJdbcBatchInterval() {
        return getIntProperty("jdbc.batch.interval", "200");
    }

    public int getJdbcMaxRetries() {
        return getIntProperty("jdbc.max.retries", "3");
    }

    // HikariCP DataSource
    public synchronized DataSource getHikariDataSource() {
        if (hikariDataSource == null) {
            int attempts = 0;
            while (attempts < MAX_RETRY_ATTEMPTS) {
                try {
                    HikariConfig config = new HikariConfig();
                    config.setJdbcUrl(getJdbcUrl());
                    config.setUsername(getJdbcUsername());
                    config.setPassword(getJdbcPassword());
                    config.setDriverClassName(getJdbcDriverClassName());

                    // 减小连接池大小，避免过多连接
                    config.setMaximumPoolSize(getJdbcMaxPoolSize());
                    config.setMinimumIdle(getJdbcMinPoolSize());
                    config.setConnectionTimeout(getJdbcConnectionTimeout());
                    config.setIdleTimeout(getJdbcIdleTimeout());
                    config.setMaxLifetime(getJdbcMaxLifetime());

                    // 添加连接池监控和诊断
                    config.setPoolName("HikariPool-Aggregator");
                    config.setLeakDetectionThreshold(60000); // 60 seconds

                    // 性能优化配置
                    config.addDataSourceProperty("cachePrepStmts", "true");
                    config.addDataSourceProperty("prepStmtCacheSize", "250");
                    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
                    config.addDataSourceProperty("useServerPrepStmts", "true");

                    // 添加重试和自动重连配置
                    config.setAutoCommit(true);
                    config.setInitializationFailTimeout(-1); // 禁用初始化超时
                    config.setKeepaliveTime(30000); // 30 seconds

                    hikariDataSource = new HikariDataSource(config);
                    LOG.info("Successfully initialized database connection pool");
                    break;
                } catch (Exception e) {
                    attempts++;
                    if (attempts >= MAX_RETRY_ATTEMPTS) {
                        LOG.error("Failed to initialize database connection pool after {} attempts", MAX_RETRY_ATTEMPTS,
                                e);
                        throw new RuntimeException("Failed to initialize database connection pool", e);
                    }
                    LOG.warn("Failed to initialize database connection pool, attempt {}/{}, retrying in {} ms",
                            attempts, MAX_RETRY_ATTEMPTS, RETRY_DELAY_MS, e);
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting to retry database connection", ie);
                    }
                }
            }
        }
        return hikariDataSource;
    }

    public synchronized void close() {
        if (hikariDataSource != null) {
            try {
                hikariDataSource.close();
            } catch (Exception e) {
                LOG.error("Error closing database connection pool", e);
            } finally {
                hikariDataSource = null;
            }
        }
    }

}
