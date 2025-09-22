package com.twilight.backend.repository;

import com.twilight.backend.model.TokenInfo;
import com.twilight.backend.model.TokenMetrics;
import com.twilight.backend.model.TokenPriceHistory;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 代币数据访问接口
 */
public interface TokenRepository {

    /**
     * 根据ID获取代币信息（PostgreSQL）
     * 
     * @param tokenId 代币ID
     * @return 代币信息
     */
    TokenInfo findTokenById(Long tokenId);

    /**
     * 获取所有代币列表（PostgreSQL）
     * 
     * @return 代币列表
     */
    List<TokenInfo> findAllTokens();

    /**
     * 获取最新代币指标（ClickHouse）
     * 
     * @param tokenId 代币ID
     * @return 代币指标
     */
    TokenMetrics findLatestMetrics(Long tokenId);

    /**
     * 获取代币历史价格（ClickHouse）
     * 
     * @param tokenId 代币ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 历史价格列表
     */
    List<TokenPriceHistory> findPriceHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取代币历史价格（按时间窗口）
     * 
     * @param tokenId 代币ID
     * @param timeWindow 时间窗口
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 历史价格列表
     */
    List<TokenPriceHistory> findPriceHistoryByWindow(Long tokenId, String timeWindow, 
                                                   LocalDateTime startTime, LocalDateTime endTime);
}
