package com.twilight.backend.service;

import com.twilight.backend.model.TokenInfo;
import com.twilight.backend.model.TokenMetrics;
import com.twilight.backend.model.TokenPriceHistory;
import com.twilight.backend.repository.TokenRepository;
import com.twilight.backend.util.TimeRangeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * 代币服务类
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TokenService {

    private final TokenRepository tokenRepository;
    private final TimeRangeUtil timeRangeUtil;

    /**
     * 获取代币基础信息
     * 
     * @param tokenId 代币ID
     * @return 代币信息
     */
    public TokenInfo getTokenInfo(Long tokenId) {
        log.debug("获取代币信息, tokenId: {}", tokenId);
        
        TokenInfo tokenInfo = tokenRepository.findTokenById(tokenId);
        if (tokenInfo == null) {
            log.warn("代币不存在, tokenId: {}", tokenId);
            return null;
        }
        
        return tokenInfo;
    }

    /**
     * 获取所有代币列表
     * 
     * @return 代币列表
     */
    public List<TokenInfo> getAllTokens() {
        log.debug("获取所有代币列表");
        return tokenRepository.findAllTokens();
    }

    /**
     * 获取代币宏观指标
     * 
     * @param tokenId 代币ID
     * @return 代币指标
     */
    public TokenMetrics getTokenMetrics(Long tokenId) {
        log.debug("获取代币宏观指标, tokenId: {}", tokenId);
        
        TokenMetrics metrics = tokenRepository.findLatestMetrics(tokenId);
        if (metrics == null) {
            log.warn("代币指标不存在, tokenId: {}", tokenId);
            return null;
        }
        
        return metrics;
    }

    /**
     * 获取代币历史价格
     * 
     * @param tokenId 代币ID
     * @param timeRange 时间范围
     * @return 历史价格列表
     */
    public List<TokenPriceHistory> getPriceHistory(Long tokenId, String timeRange) {
        log.debug("获取代币历史价格, tokenId: {}, timeRange: {}", tokenId, timeRange);
        
        TimeRangeUtil.TimeRange range = timeRangeUtil.parseTimeRange(timeRange);
        if (!timeRangeUtil.isValidTimeRange(range)) {
            log.error("无效的时间范围: {}", timeRange);
            return List.of();
        }
        
        // 根据时间跨度选择合适的聚合窗口
        String timeWindow = timeRangeUtil.getSuggestedAggregationWindow(range);
        
        List<TokenPriceHistory> history = tokenRepository.findPriceHistoryByWindow(
            tokenId, timeWindow, range.getStartTime(), range.getEndTime());
        
        // 计算价格变化
        calculatePriceChanges(history);
        
        return history;
    }

    /**
     * 计算价格变化量和百分比
     * 
     * @param history 价格历史列表
     */
    private void calculatePriceChanges(List<TokenPriceHistory> history) {
        if (history == null || history.size() < 2) {
            return;
        }
        
        for (int i = 1; i < history.size(); i++) {
            TokenPriceHistory current = history.get(i);
            TokenPriceHistory previous = history.get(i - 1);
            
            if (current.getPrice() != null && previous.getPrice() != null) {
                // 计算价格变化量
                BigDecimal priceChange = current.getPrice().subtract(previous.getPrice());
                current.setPriceChange(priceChange);
                
                // 计算价格变化百分比
                if (previous.getPrice().compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal changePercent = priceChange
                        .divide(previous.getPrice(), 6, RoundingMode.HALF_UP)
                        .multiply(BigDecimal.valueOf(100));
                    current.setPriceChangePercent(changePercent);
                }
            }
        }
        
        // 第一个数据点的变化设为0
        if (!history.isEmpty()) {
            TokenPriceHistory first = history.get(0);
            first.setPriceChange(BigDecimal.ZERO);
            first.setPriceChangePercent(BigDecimal.ZERO);
        }
    }
}
