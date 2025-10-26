package com.twilight.backend.service;

import com.twilight.backend.model.ApiResponse;
import com.twilight.backend.model.TokenDistribution;
import com.twilight.backend.model.TokenOverview;
import com.twilight.backend.model.TokenPnL;
import com.twilight.backend.repository.TokenRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket数据推送服务
 * 管理活跃订阅并定期推送最新数据
 */
@Slf4j
@Service
public class WebSocketPushService {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private TokenRepository tokenRepository;

    // 存储活跃的订阅: tokenId -> SubscriptionInfo
    private final Map<Long, SubscriptionInfo> activeOverviewSubscriptions = new ConcurrentHashMap<>();
    private final Map<Long, SubscriptionInfo> activeDistributionSubscriptions = new ConcurrentHashMap<>();
    private final Map<Long, PnLSubscriptionInfo> activePnLSubscriptions = new ConcurrentHashMap<>();

    /**
     * 订阅信息
     */
    private static class SubscriptionInfo {
        String timeRange;
        long lastUpdateTime;

        SubscriptionInfo(String timeRange) {
            this.timeRange = timeRange;
            this.lastUpdateTime = System.currentTimeMillis();
        }
    }

    /**
     * PnL订阅信息
     */
    private static class PnLSubscriptionInfo extends SubscriptionInfo {
        int topLimit;

        PnLSubscriptionInfo(String timeRange, int topLimit) {
            super(timeRange);
            this.topLimit = topLimit;
        }
    }

    /**
     * 注册代币概览订阅
     */
    public void registerOverviewSubscription(Long tokenId, String timeRange) {
        activeOverviewSubscriptions.put(tokenId, new SubscriptionInfo(timeRange));
        log.info("注册代币概览订阅: tokenId={}, timeRange={}", tokenId, timeRange);
    }

    /**
     * 注册代币分布订阅
     */
    public void registerDistributionSubscription(Long tokenId, String timeRange) {
        activeDistributionSubscriptions.put(tokenId, new SubscriptionInfo(timeRange));
        log.info("注册代币分布订阅: tokenId={}, timeRange={}", tokenId, timeRange);
    }

    /**
     * 注册代币PnL订阅
     */
    public void registerPnLSubscription(Long tokenId, String timeRange, int topLimit) {
        activePnLSubscriptions.put(tokenId, new PnLSubscriptionInfo(timeRange, topLimit));
        log.info("注册代币PnL订阅: tokenId={}, timeRange={}, topLimit={}", tokenId, timeRange, topLimit);
    }

    /**
     * 取消代币概览订阅
     */
    public void unregisterOverviewSubscription(Long tokenId) {
        activeOverviewSubscriptions.remove(tokenId);
        log.info("取消代币概览订阅: tokenId={}", tokenId);
    }

    /**
     * 取消代币分布订阅
     */
    public void unregisterDistributionSubscription(Long tokenId) {
        activeDistributionSubscriptions.remove(tokenId);
        log.info("取消代币分布订阅: tokenId={}", tokenId);
    }

    /**
     * 取消代币PnL订阅
     */
    public void unregisterPnLSubscription(Long tokenId) {
        activePnLSubscriptions.remove(tokenId);
        log.info("取消代币PnL订阅: tokenId={}", tokenId);
    }

    /**
     * 定时推送代币概览数据 - 每5秒执行一次
     */
    @Scheduled(fixedRate = 5000)
    public void pushTokenOverviewUpdates() {
        if (activeOverviewSubscriptions.isEmpty()) {
            return;
        }

        log.debug("推送代币概览更新, 活跃订阅数: {}", activeOverviewSubscriptions.size());
        
        Set<Long> tokenIds = activeOverviewSubscriptions.keySet();
        for (Long tokenId : tokenIds) {
            try {
                SubscriptionInfo info = activeOverviewSubscriptions.get(tokenId);
                if (info == null) {
                    continue;
                }

                TokenOverview overview = tokenRepository.findTokenOverview(tokenId, info.timeRange);
                if (overview != null) {
                    String destination = "/topic/analytics/tokens/" + tokenId + "/overview";
                    messagingTemplate.convertAndSend(destination, ApiResponse.success(overview));
                    info.lastUpdateTime = System.currentTimeMillis();
                    log.debug("推送代币概览成功: tokenId={}, destination={}", tokenId, destination);
                }
            } catch (Exception e) {
                log.error("推送代币概览失败: tokenId={}", tokenId, e);
            }
        }
    }

    /**
     * 定时推送代币分布数据 - 每5秒执行一次
     */
    @Scheduled(fixedRate = 5000)
    public void pushTokenDistributionUpdates() {
        if (activeDistributionSubscriptions.isEmpty()) {
            return;
        }

        log.debug("推送代币分布更新, 活跃订阅数: {}", activeDistributionSubscriptions.size());
        
        Set<Long> tokenIds = activeDistributionSubscriptions.keySet();
        for (Long tokenId : tokenIds) {
            try {
                SubscriptionInfo info = activeDistributionSubscriptions.get(tokenId);
                if (info == null) {
                    continue;
                }

                TokenDistribution distribution = tokenRepository.findTokenDistribution(tokenId, info.timeRange);
                if (distribution != null) {
                    String destination = "/topic/analytics/tokens/" + tokenId + "/distribution";
                    messagingTemplate.convertAndSend(destination, ApiResponse.success(distribution));
                    info.lastUpdateTime = System.currentTimeMillis();
                    log.debug("推送代币分布成功: tokenId={}, destination={}", tokenId, destination);
                }
            } catch (Exception e) {
                log.error("推送代币分布失败: tokenId={}", tokenId, e);
            }
        }
    }

    /**
     * 定时推送代币PnL数据 - 每5秒执行一次
     */
    @Scheduled(fixedRate = 5000)
    public void pushTokenPnLUpdates() {
        if (activePnLSubscriptions.isEmpty()) {
            return;
        }

        log.debug("推送代币PnL更新, 活跃订阅数: {}", activePnLSubscriptions.size());
        
        Set<Long> tokenIds = activePnLSubscriptions.keySet();
        for (Long tokenId : tokenIds) {
            try {
                PnLSubscriptionInfo info = activePnLSubscriptions.get(tokenId);
                if (info == null) {
                    continue;
                }

                TokenPnL tokenPnL = tokenRepository.findTokenPnL(tokenId, info.timeRange, info.topLimit);
                if (tokenPnL != null) {
                    String destination = "/topic/analytics/tokens/" + tokenId + "/pnl";
                    messagingTemplate.convertAndSend(destination, ApiResponse.success(tokenPnL));
                    info.lastUpdateTime = System.currentTimeMillis();
                    log.debug("推送代币PnL成功: tokenId={}, destination={}", tokenId, destination);
                }
            } catch (Exception e) {
                log.error("推送代币PnL失败: tokenId={}", tokenId, e);
            }
        }
    }

    /**
     * 清理长时间未更新的订阅 - 每分钟执行一次
     * 超过2分钟未更新的订阅将被清理
     */
    @Scheduled(fixedRate = 60000)
    public void cleanupInactiveSubscriptions() {
        long now = System.currentTimeMillis();
        long timeout = 120000; // 2分钟超时

        // 清理概览订阅
        activeOverviewSubscriptions.entrySet().removeIf(entry -> {
            boolean expired = (now - entry.getValue().lastUpdateTime) > timeout;
            if (expired) {
                log.info("清理过期的代币概览订阅: tokenId={}", entry.getKey());
            }
            return expired;
        });

        // 清理分布订阅
        activeDistributionSubscriptions.entrySet().removeIf(entry -> {
            boolean expired = (now - entry.getValue().lastUpdateTime) > timeout;
            if (expired) {
                log.info("清理过期的代币分布订阅: tokenId={}", entry.getKey());
            }
            return expired;
        });

        // 清理PnL订阅
        activePnLSubscriptions.entrySet().removeIf(entry -> {
            boolean expired = (now - entry.getValue().lastUpdateTime) > timeout;
            if (expired) {
                log.info("清理过期的代币PnL订阅: tokenId={}", entry.getKey());
            }
            return expired;
        });
    }
}

