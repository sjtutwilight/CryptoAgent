package com.twilight.backend.repository;

import com.twilight.backend.model.DexTradeDetail;
import com.twilight.backend.model.PageResult;
import com.twilight.backend.model.TagNetFlow;
import com.twilight.backend.model.TokenTradeVolume;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 交易数据访问接口
 */
public interface TradeRepository {

    /**
     * 获取代币交易量统计
     * 
     * @param tokenId 代币ID
     * @param timeWindow 时间窗口
     * @param tag 标签
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 交易量统计列表
     */
    List<TokenTradeVolume> findTradeVolume(Long tokenId, String timeWindow, String tag, 
                                         LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 计算标签净流入
     * 
     * @param tokenId 代币ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 标签净流入列表
     */
    List<TagNetFlow> calculateNetFlow(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取DEX交易明细（按代币）
     * 
     * @param tokenId 代币ID
     * @param offset 偏移量
     * @param limit 限制数量
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 分页交易明细
     */
    PageResult<DexTradeDetail> findDexTradesByToken(Long tokenId, Integer offset, Integer limit, 
                                                  LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取DEX交易明细（按账户）
     * 
     * @param accountId 账户ID
     * @param offset 偏移量
     * @param limit 限制数量
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 分页交易明细
     */
    PageResult<DexTradeDetail> findDexTradesByAccount(Long accountId, Integer offset, Integer limit, 
                                                    LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取代币交易明细总数
     * 
     * @param tokenId 代币ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 总数
     */
    Long countDexTradesByToken(Long tokenId, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取账户交易明细总数
     * 
     * @param accountId 账户ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 总数
     */
    Long countDexTradesByAccount(Long accountId, LocalDateTime startTime, LocalDateTime endTime);
}
