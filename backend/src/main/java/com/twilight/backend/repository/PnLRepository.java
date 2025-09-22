package com.twilight.backend.repository;

import com.twilight.backend.model.MacroPnLMetrics;
import com.twilight.backend.model.TopPnLInfo;

import java.time.LocalDateTime;
import java.util.List;

/**
 * PnL数据访问接口
 */
public interface PnLRepository {

    /**
     * 获取Top PnL信息
     * 
     * @param tokenId 代币ID
     * @param limit 限制数量
     * @return Top PnL列表
     */
    List<TopPnLInfo> findTopPnL(Long tokenId, Integer limit);

    /**
     * 获取最新宏观PnL指标
     * 
     * @param tokenId 代币ID
     * @return 宏观PnL指标
     */
    MacroPnLMetrics findLatestMacroPnL(Long tokenId);

    /**
     * 获取宏观PnL指标历史
     * 
     * @param tokenId 代币ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 宏观PnL指标历史列表
     */
    List<MacroPnLMetrics> findMacroPnLHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取账户在特定代币的PnL信息
     * 
     * @param accountId 账户ID
     * @param tokenId 代币ID
     * @return PnL信息
     */
    TopPnLInfo findAccountPnL(Long accountId, Long tokenId);

    /**
     * 获取账户的所有代币PnL信息
     * 
     * @param accountId 账户ID
     * @param limit 限制数量
     * @return PnL信息列表
     */
    List<TopPnLInfo> findAccountAllPnL(Long accountId, Integer limit);
}
