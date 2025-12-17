package com.twilight.backend.config;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Prometheus Metrics 配置类
 * 提供自定义业务指标的注册和管理
 */
@Configuration
public class MetricsConfig {

    /**
     * 启用 @Timed 注解支持
     * 可以在Controller/Service方法上使用 @Timed 自动记录执行时间
     */
    @Bean
    public TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }

    /**
     * 自定义业务指标注册
     */
    @Bean
    public MeterBinder customMetrics() {
        return registry -> {
            // ==================== API 请求指标 ====================
            
            // API 请求计数器（按endpoint分组）
            Counter.builder("twilight_api_requests_total")
                    .description("API请求总数")
                    .tag("endpoint", "all")
                    .register(registry);

            // API 错误计数器
            Counter.builder("twilight_api_errors_total")
                    .description("API错误总数")
                    .tag("endpoint", "all")
                    .tag("error_type", "unknown")
                    .register(registry);

            // ==================== 数据库查询指标 ====================
            
            // ClickHouse 查询计数
            Counter.builder("twilight_clickhouse_queries_total")
                    .description("ClickHouse查询总数")
                    .tag("query_type", "select")
                    .register(registry);

            // PostgreSQL 查询计数
            Counter.builder("twilight_postgres_queries_total")
                    .description("PostgreSQL查询总数")
                    .tag("query_type", "select")
                    .register(registry);
        };
    }

    /**
     * API 指标帮助类
     * 提供便捷的指标记录方法
     */
    @Bean
    public ApiMetrics apiMetrics(MeterRegistry registry) {
        return new ApiMetrics(registry);
    }

    /**
     * API 指标记录器
     */
    public static class ApiMetrics {
        private final MeterRegistry registry;

        public ApiMetrics(MeterRegistry registry) {
            this.registry = registry;
        }

        /**
         * 记录API请求
         */
        public void recordRequest(String endpoint, String method, int statusCode) {
            Counter.builder("twilight_api_requests_total")
                    .description("API请求总数")
                    .tag("endpoint", endpoint)
                    .tag("method", method)
                    .tag("status", String.valueOf(statusCode))
                    .register(registry)
                    .increment();
        }

        /**
         * 记录API错误
         */
        public void recordError(String endpoint, String errorType) {
            Counter.builder("twilight_api_errors_total")
                    .description("API错误总数")
                    .tag("endpoint", endpoint)
                    .tag("error_type", errorType)
                    .register(registry)
                    .increment();
        }

        /**
         * 记录API响应时间
         */
        public Timer.Sample startTimer() {
            return Timer.start(registry);
        }

        /**
         * 停止计时并记录
         */
        public void stopTimer(Timer.Sample sample, String endpoint, String method) {
            sample.stop(Timer.builder("twilight_api_latency_seconds")
                    .description("API响应延迟")
                    .tag("endpoint", endpoint)
                    .tag("method", method)
                    .register(registry));
        }

        /**
         * 记录ClickHouse查询
         */
        public void recordClickHouseQuery(String queryType, boolean success) {
            Counter.builder("twilight_clickhouse_queries_total")
                    .description("ClickHouse查询总数")
                    .tag("query_type", queryType)
                    .tag("success", String.valueOf(success))
                    .register(registry)
                    .increment();
        }

        /**
         * 记录ClickHouse查询耗时
         */
        public void recordClickHouseLatency(String queryType, long durationMs) {
            Timer.builder("twilight_clickhouse_query_latency_seconds")
                    .description("ClickHouse查询延迟")
                    .tag("query_type", queryType)
                    .register(registry)
                    .record(java.time.Duration.ofMillis(durationMs));
        }

        /**
         * 记录PostgreSQL查询
         */
        public void recordPostgresQuery(String queryType, boolean success) {
            Counter.builder("twilight_postgres_queries_total")
                    .description("PostgreSQL查询总数")
                    .tag("query_type", queryType)
                    .tag("success", String.valueOf(success))
                    .register(registry)
                    .increment();
        }
    }
}





