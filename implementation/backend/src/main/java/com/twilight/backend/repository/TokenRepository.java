package com.twilight.backend.repository;

import com.twilight.backend.model.TokenListItem;
import com.twilight.backend.model.TokenOverview;
import com.twilight.backend.model.TokenDistribution;
import com.twilight.backend.model.TokenPnL;

import java.util.List;

/**
 * 代币数据访问接口
 */
public interface TokenRepository {

    /**
     * 获取代币列表（用于前端展示）
     * 
     * @param page 页码
     * @param pageSize 页大小
     * @param sortBy 排序字段 (mcap, volume, price, change1m)
     * @param order 排序方向 (asc, desc)
     * @return 代币列表项
     */
    List<TokenListItem> findTokenListItems(Integer page, Integer pageSize, String sortBy, String order);

    /**
     * 获取代币列表总数
     * 
     * @return 总数
     */
    Long countTokenListItems();
    
    /**
     * 获取代币概览信息
     * 
     * @param tokenId 代币ID
     * @param timeRange 时间范围
     * @return 代币概览数据
     */
    TokenOverview findTokenOverview(Long tokenId, String timeRange);
    
    /**
     * 获取代币分布信息
     * 
     * @param tokenId 代币ID
     * @param timeRange 时间范围
     * @return 代币分布数据
     */
    TokenDistribution findTokenDistribution(Long tokenId, String timeRange);
    
    /**
     * 获取代币PnL分析数据
     * 
     * @param tokenId 代币ID
     * @param timeRange 时间范围
     * @param topLimit Top PnL排行榜数量限制
     * @return PnL分析数据
     */
    TokenPnL findTokenPnL(Long tokenId, String timeRange, Integer topLimit);
}