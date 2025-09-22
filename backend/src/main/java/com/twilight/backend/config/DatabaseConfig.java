package com.twilight.backend.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * 数据库配置类
 * 配置ClickHouse和PostgreSQL双数据源
 */
@Configuration
public class DatabaseConfig {

    /**
     * ClickHouse数据源 - 主要查询数据源
     */
    @Primary
    @Bean(name = "clickhouseDataSource")
    @ConfigurationProperties("spring.datasource.clickhouse")
    public DataSource clickhouseDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * PostgreSQL数据源 - 元数据存储
     */
    @Bean(name = "postgresqlDataSource")
    @ConfigurationProperties("spring.datasource.postgresql")
    public DataSource postgresqlDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * ClickHouse JdbcTemplate
     */
    @Primary
    @Bean(name = "clickhouseJdbcTemplate")
    public JdbcTemplate clickhouseJdbcTemplate(@Qualifier("clickhouseDataSource") DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        // 设置查询超时时间为30秒
        jdbcTemplate.setQueryTimeout(30);
        return jdbcTemplate;
    }

    /**
     * PostgreSQL JdbcTemplate  
     */
    @Bean(name = "postgresqlJdbcTemplate")
    public JdbcTemplate postgresqlJdbcTemplate(@Qualifier("postgresqlDataSource") DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setQueryTimeout(10);
        return jdbcTemplate;
    }
}
