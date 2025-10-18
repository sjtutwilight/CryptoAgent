package com.crypto.control.service;

import com.crypto.control.config.DataSourceConfigProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * 基于Redis滑动窗口的限流服务
 */
@Slf4j
@Service
public class RateLimiterService {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Value("${app.rate-limit.redis-key-prefix:rate-limit:}")
    private String redisKeyPrefix;
    
    @Value("${app.rate-limit.window-size:60}")
    private int defaultWindowSize;

    @Value("${app.rate-limit.max-requests:100}")
    private int defaultMaxRequests;
    
    // Lua脚本：滑动窗口限流
    private static final String LUA_SCRIPT = """
            local key = KEYS[1]
            local window = tonumber(ARGV[1])
            local limit = tonumber(ARGV[2])
            local cost = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])
            
            -- 移除过期元素
            redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window * 1000)
            
            -- 获取当前窗口内的请求数
            local current = redis.call('ZCARD', key)
            
            -- 检查是否超过限制
            if current + cost > limit then
                return {0, current, limit}
            end
            
            -- 添加当前请求
            redis.call('ZADD', key, now, now .. ':' .. math.random(1000000))
            redis.call('EXPIRE', key, window + 1)
            
            return {1, current + cost, limit}
            """;
    
    private final DefaultRedisScript<Object> rateLimitScript;
    
    public RateLimiterService() {
        this.rateLimitScript = new DefaultRedisScript<>();
        this.rateLimitScript.setScriptText(LUA_SCRIPT);
        this.rateLimitScript.setResultType(Object.class);
    }
    
    /**
     * 检查是否允许请求通过
     * 
     * @param dataSourceConfig 数据源配置
     * @param cost 请求成本
     * @return 限流结果
     */
    public RateLimitResult checkRateLimit(DataSourceConfigProperties.DataSourceConfig dataSourceConfig, int cost) {
        return checkRateLimit(
                dataSourceConfig.getDataSourceId(),
                dataSourceConfig.getRateLimitWeight(),
                dataSourceConfig.getRateLimitInterval(),
                cost
        );
    }
    
    /**
     * 检查是否允许请求通过
     * 
     * @param key 限流键
     * @param maxRequests 最大请求数
     * @param windowSeconds 时间窗口（秒）
     * @param cost 请求成本
     * @return 限流结果
     */
    public RateLimitResult checkRateLimit(String key, int maxRequests, int windowSeconds, int cost) {
        try {
            String redisKey = redisKeyPrefix + key;
            long now = System.currentTimeMillis();
            
            // 执行Lua脚本
            Object result = redisTemplate.execute(
                    rateLimitScript,
                    Collections.singletonList(redisKey),
                    String.valueOf(windowSeconds),
                    String.valueOf(maxRequests),
                    String.valueOf(cost),
                    String.valueOf(now)
            );
            
            if (result instanceof java.util.List) {
                @SuppressWarnings("unchecked")
                java.util.List<Object> list = (java.util.List<Object>) result;
                
                boolean allowed = ((Number) list.get(0)).longValue() == 1;
                long currentRequests = ((Number) list.get(1)).longValue();
                long totalLimit = ((Number) list.get(2)).longValue();
                
                LocalDateTime resetTime = LocalDateTime.now().plusSeconds(windowSeconds);
                
                RateLimitResult rateLimitResult = new RateLimitResult(
                        allowed,
                        currentRequests,
                        totalLimit,
                        resetTime
                );
                
                if (allowed) {
                    log.debug("限流检查通过: key={}, current={}/{}, cost={}", 
                            key, currentRequests, totalLimit, cost);
                } else {
                    log.warn("限流检查失败: key={}, current={}/{}, cost={}, resetTime={}", 
                            key, currentRequests, totalLimit, cost, resetTime);
                }
                
                return rateLimitResult;
            }
            
            // 如果Redis返回结果格式异常，默认拒绝请求
            log.error("Redis限流脚本返回格式异常: key={}, result={}", key, result);
            return RateLimitResult.rejected(0, maxRequests, 
                    LocalDateTime.now().plusSeconds(windowSeconds));
            
        } catch (Exception e) {
            log.error("限流检查异常: key={}, error={}", key, e.getMessage(), e);
            
            // 发生异常时，默认允许请求通过（故障开放模式）
            return RateLimitResult.allowed(0, maxRequests, 
                    LocalDateTime.now().plusSeconds(windowSeconds));
        }
    }
    
    /**
     * 获取当前限流状态
     */
    public RateLimitStatus getCurrentStatus(String key) {
        try {
            String redisKey = redisKeyPrefix + key;
            Long currentRequests = redisTemplate.opsForZSet().count(redisKey, 
                    System.currentTimeMillis() - defaultWindowSize * 1000L, 
                    System.currentTimeMillis());
            
            return new RateLimitStatus(
                    key,
                    currentRequests != null ? currentRequests : 0,
                    defaultMaxRequests,
                    defaultWindowSize,
                    LocalDateTime.now()
            );
            
        } catch (Exception e) {
            log.error("获取限流状态异常: key={}, error={}", key, e.getMessage(), e);
            return new RateLimitStatus(key, 0, defaultMaxRequests, defaultWindowSize, LocalDateTime.now());
        }
    }
    
    /**
     * 清理过期的限流记录
     */
    public void cleanupExpiredRecords() {
        try {
            // 这里可以实现定期清理逻辑
            // Redis的EXPIRE已经可以自动清理，这里主要用于监控和统计
            log.debug("清理过期限流记录完成");
        } catch (Exception e) {
            log.error("清理过期限流记录失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 限流结果
     */
    public static class RateLimitResult {
        private final boolean allowed;
        private final long currentRequests;
        private final long totalLimit;
        private final LocalDateTime resetTime;
        
        public RateLimitResult(boolean allowed, long currentRequests, long totalLimit, LocalDateTime resetTime) {
            this.allowed = allowed;
            this.currentRequests = currentRequests;
            this.totalLimit = totalLimit;
            this.resetTime = resetTime;
        }
        
        public static RateLimitResult allowed(long currentRequests, long totalLimit, LocalDateTime resetTime) {
            return new RateLimitResult(true, currentRequests, totalLimit, resetTime);
        }
        
        public static RateLimitResult rejected(long currentRequests, long totalLimit, LocalDateTime resetTime) {
            return new RateLimitResult(false, currentRequests, totalLimit, resetTime);
        }
        
        public boolean isAllowed() { return allowed; }
        public long getCurrentRequests() { return currentRequests; }
        public long getTotalLimit() { return totalLimit; }
        public LocalDateTime getResetTime() { return resetTime; }
        
        public long getRetryAfterSeconds() {
            return resetTime.toEpochSecond(ZoneOffset.UTC) - LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        }
    }
    
    /**
     * 限流状态
     */
    public record RateLimitStatus(
            String key,
            long currentRequests,
            long totalLimit,
            int windowSeconds,
            LocalDateTime lastUpdateTime
    ) {}
}