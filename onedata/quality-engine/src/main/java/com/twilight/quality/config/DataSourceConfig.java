package com.twilight.quality.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * 多数据源配置
 * PostgreSQL: JPA实体存储（告警记录、规则配置）
 * ClickHouse: 时序指标存储
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
        basePackages = "com.twilight.quality.repository",
        entityManagerFactoryRef = "postgresEntityManagerFactory",
        transactionManagerRef = "postgresTransactionManager"
)
public class DataSourceConfig {
    
    // ==================== PostgreSQL 配置 ====================
    
    /**
     * PostgreSQL数据源配置属性
     */
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.postgres")
    public PostgresProperties postgresProperties() {
        return new PostgresProperties();
    }
    
    /**
     * PostgreSQL数据源 - 主数据源，用于JPA
     */
    @Primary
    @Bean(name = "postgresDataSource")
    public DataSource postgresDataSource(PostgresProperties props) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("PostgresPool");
        config.setJdbcUrl(props.getUrl());
        config.setUsername(props.getUsername());
        config.setPassword(props.getPassword());
        config.setDriverClassName("org.postgresql.Driver");
        config.setMaximumPoolSize(props.getMaxPoolSize());
        config.setMinimumIdle(props.getMinIdle());
        return new HikariDataSource(config);
    }
    
    /**
     * PostgreSQL EntityManagerFactory
     */
    @Primary
    @Bean(name = "postgresEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean postgresEntityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier("postgresDataSource") DataSource dataSource) {
        
        Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        properties.put("hibernate.hbm2ddl.auto", "update");
        properties.put("hibernate.show_sql", "false");
        
        return builder
                .dataSource(dataSource)
                .packages("com.twilight.quality.domain.entity")
                .persistenceUnit("postgres")
                .properties(properties)
                .build();
    }
    
    /**
     * PostgreSQL事务管理器
     */
    @Primary
    @Bean(name = "postgresTransactionManager")
    public PlatformTransactionManager postgresTransactionManager(
            @Qualifier("postgresEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
    
    // ==================== ClickHouse 配置 ====================
    
    /**
     * ClickHouse配置属性
     */
    @Bean
    @ConfigurationProperties(prefix = "clickhouse")
    public ClickHouseProperties clickHouseProperties() {
        return new ClickHouseProperties();
    }
    
    /**
     * ClickHouse数据源 - 仅用于指标写入，不参与JPA
     */
    @Bean(name = "clickHouseDataSource")
    public DataSource clickHouseDataSource(ClickHouseProperties props) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("ClickHousePool");
        config.setJdbcUrl(props.getUrl());
        config.setUsername(props.getUsername());
        config.setPassword(props.getPassword());
        config.setDriverClassName(props.getDriverClassName());
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(30000);
        config.setAutoCommit(true);
        
        // ClickHouse特定配置
        config.addDataSourceProperty("socket_timeout", "30000");
        config.addDataSourceProperty("connection_timeout", "10000");
        
        HikariDataSource dataSource = new HikariDataSource(config);
        
        // 初始化表结构
        initClickHouseTables(dataSource);
        
        return dataSource;
    }
    
    /**
     * 初始化ClickHouse表结构
     */
    private void initClickHouseTables(DataSource dataSource) {
        String createMetricsTable = """
            CREATE TABLE IF NOT EXISTS quality_metrics (
                metric_id String,
                domain LowCardinality(String),
                stream_key String,
                dimension LowCardinality(String),
                rule_name LowCardinality(String),
                value Float64,
                threshold Float64,
                passed UInt8,
                window_start DateTime64(3),
                window_end DateTime64(3),
                message_count UInt64,
                collected_at DateTime64(3)
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(collected_at)
            ORDER BY (domain, stream_key, rule_name, collected_at)
            TTL collected_at + INTERVAL 30 DAY
            SETTINGS index_granularity = 8192
            """;
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(createMetricsTable);
        } catch (SQLException e) {
            // 忽略，表可能已存在
        }
    }
    
    // ==================== 配置属性类 ====================
    
    /**
     * PostgreSQL配置属性
     */
    public static class PostgresProperties {
        private String url = "jdbc:postgresql://localhost:5432/twilight";
        private String username = "twilight";
        private String password = "twilight123";
        private int maxPoolSize = 10;
        private int minIdle = 2;
        
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public int getMaxPoolSize() { return maxPoolSize; }
        public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }
        public int getMinIdle() { return minIdle; }
        public void setMinIdle(int minIdle) { this.minIdle = minIdle; }
    }
    
    /**
     * ClickHouse配置属性
     */
    public static class ClickHouseProperties {
        private String url = "jdbc:clickhouse://localhost:8123/default";
        private String username = "default";
        private String password = "";
        private String driverClassName = "com.clickhouse.jdbc.ClickHouseDriver";
        
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getDriverClassName() { return driverClassName; }
        public void setDriverClassName(String driverClassName) { this.driverClassName = driverClassName; }
    }
}

