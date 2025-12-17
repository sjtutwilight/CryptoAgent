package com.twilight.quality.aggregator;

import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.RuleEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 窗口管理器
 * 负责管理聚合规则的窗口状态
 */
@Component
public class WindowManager {
    
    private static final Logger log = LoggerFactory.getLogger(WindowManager.class);
    
    /**
     * 窗口状态存储
     * Key: windowKey (domain:streamKey:ruleName:windowStart)
     */
    private final Map<String, WindowState> windows = new ConcurrentHashMap<>();
    
    /**
     * 窗口结果回调
     */
    private Consumer<RuleResult> resultCallback;
    
    @Lazy
    private final RuleEngine ruleEngine;
    
    @Value("${quality.window.default-size-ms:60000}")
    private long defaultWindowSizeMs;
    
    public WindowManager(@Lazy RuleEngine ruleEngine) {
        this.ruleEngine = ruleEngine;
    }
    
    /**
     * 设置结果回调
     */
    public void setResultCallback(Consumer<RuleResult> callback) {
        this.resultCallback = callback;
    }
    
    /**
     * 获取或创建窗口状态
     */
    public WindowState getOrCreateWindow(DataDomain domain, String streamKey, 
                                          String ruleName, long windowSizeMs) {
        long currentTime = System.currentTimeMillis();
        long windowStart = (currentTime / windowSizeMs) * windowSizeMs;
        long windowEnd = windowStart + windowSizeMs;
        
        String windowKey = buildWindowKey(domain, streamKey, ruleName, windowStart);
        
        return windows.computeIfAbsent(windowKey, k -> {
            log.debug("创建新窗口: {} [{} - {}]", windowKey, windowStart, windowEnd);
            return new WindowState(domain, streamKey, ruleName, windowStart, windowEnd);
        });
    }
    
    /**
     * 构建窗口Key
     */
    private String buildWindowKey(DataDomain domain, String streamKey, 
                                   String ruleName, long windowStart) {
        return String.format("%s:%s:%s:%d", domain.getDomainId(), streamKey, ruleName, windowStart);
    }
    
    /**
     * 定时检查并关闭过期窗口
     */
    @Scheduled(fixedDelayString = "${quality.window.flush-interval-ms:10000}")
    public void flushExpiredWindows() {
        long currentTime = System.currentTimeMillis();
        List<String> expiredKeys = new ArrayList<>();
        List<WindowState> expiredWindows = new ArrayList<>();
        
        // 找出所有过期窗口
        for (Map.Entry<String, WindowState> entry : windows.entrySet()) {
            WindowState state = entry.getValue();
            if (state.isExpired(currentTime)) {
                expiredKeys.add(entry.getKey());
                expiredWindows.add(state);
            }
        }
        
        // 移除过期窗口
        for (String key : expiredKeys) {
            windows.remove(key);
        }
        
        // 评估过期窗口
        for (WindowState state : expiredWindows) {
            try {
                evaluateAndCallback(state);
            } catch (Exception e) {
                log.error("窗口评估失败: {}", state.getWindowKey(), e);
            }
        }
        
        if (!expiredWindows.isEmpty()) {
            log.debug("处理 {} 个过期窗口，当前活跃窗口数: {}", expiredWindows.size(), windows.size());
        }
    }
    
    /**
     * 评估窗口并回调结果
     */
    private void evaluateAndCallback(WindowState state) {
        Optional<RuleResult> result = ruleEngine.evaluateWindow(state);
        
        if (result.isPresent() && resultCallback != null) {
            resultCallback.accept(result.get());
        }
    }
    
    /**
     * 获取活跃窗口数量
     */
    public int getActiveWindowCount() {
        return windows.size();
    }
    
    /**
     * 获取指定域的活跃窗口
     */
    public List<WindowState> getWindowsForDomain(DataDomain domain) {
        List<WindowState> result = new ArrayList<>();
        for (WindowState state : windows.values()) {
            if (state.getDomain() == domain) {
                result.add(state);
            }
        }
        return result;
    }
    
    /**
     * 强制刷新所有窗口（用于关闭时）
     */
    public void flushAll() {
        log.info("强制刷新所有窗口，共 {} 个", windows.size());
        
        List<WindowState> allWindows = new ArrayList<>(windows.values());
        windows.clear();
        
        for (WindowState state : allWindows) {
            try {
                evaluateAndCallback(state);
            } catch (Exception e) {
                log.error("窗口评估失败: {}", state.getWindowKey(), e);
            }
        }
    }
    
    /**
     * 获取窗口统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("activeWindows", windows.size());
        
        Map<String, Integer> byDomain = new HashMap<>();
        Map<String, Long> messagesByDomain = new HashMap<>();
        
        for (WindowState state : windows.values()) {
            String domainId = state.getDomain().getDomainId();
            byDomain.merge(domainId, 1, Integer::sum);
            messagesByDomain.merge(domainId, state.getMessageCount(), Long::sum);
        }
        
        stats.put("windowsByDomain", byDomain);
        stats.put("messagesByDomain", messagesByDomain);
        
        return stats;
    }
}

