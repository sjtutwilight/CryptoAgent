package com.twilight.backend.repository;

import com.twilight.backend.model.TagHolding;
import com.twilight.backend.model.TokenDistribution;
import com.twilight.backend.model.TopHolder;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 代币分布数据访问接口
 */
public interface DistributionRepository {

    /**
     * 获取最新代币分布指标
     * 
     * @param tokenId 代币ID
     * @return 代币分布指标
     */
    TokenDistribution findLatestDistribution(Long tokenId);

    /**
     * 获取代币分布历史
     * 
     * @param tokenId 代币ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 代币分布历史列表
     */
    List<TokenDistribution> findDistributionHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取标签持仓情况
     * 
     * @param tokenId 代币ID
     * @param endTime 结束时间
     * @return 标签持仓列表
     */
    List<TagHolding> findTagHoldings(Long tokenId, LocalDateTime endTime);

    /**
     * 获取标签持仓历史
     * 
     * @param tokenId 代币ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 标签持仓历史列表
     */
    List<TagHolding> findTagHoldingsHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取Top Holder明细
     * 
     * @param tokenId 代币ID
     * @param limit 限制数量
     * @return Top Holder列表
     */
    List<TopHolder> findTopHolders(Long tokenId, Integer limit);

    /**
     * 获取Top Holder明细（指定时间）
     * 
     * @param tokenId 代币ID
     * @param endTime 结束时间
     * @param limit 限制数量
     * @return Top Holder列表
     */
    List<TopHolder> findTopHolders(Long tokenId, LocalDateTime endTime, Integer limit);
}
