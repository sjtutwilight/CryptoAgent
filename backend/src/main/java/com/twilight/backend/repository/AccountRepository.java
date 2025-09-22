package com.twilight.backend.repository;

import com.twilight.backend.model.AccountAsset;
import com.twilight.backend.model.AccountInfo;
import com.twilight.backend.model.AccountTransferHistory;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 账户数据访问接口
 */
public interface AccountRepository {

    /**
     * 根据ID获取账户信息（PostgreSQL）
     * 
     * @param accountId 账户ID
     * @return 账户信息
     */
    AccountInfo findAccountById(Long accountId);

    /**
     * 根据地址获取账户信息（PostgreSQL）
     * 
     * @param address 账户地址
     * @return 账户信息
     */
    AccountInfo findAccountByAddress(String address);

    /**
     * 获取所有账户列表（PostgreSQL）
     * 
     * @param offset 偏移量
     * @param limit 限制数量
     * @return 账户列表
     */
    List<AccountInfo> findAllAccounts(Integer offset, Integer limit);

    /**
     * 获取账户总数
     * 
     * @return 总数
     */
    Long countAccounts();

    /**
     * 获取账户资产（ClickHouse）
     * 
     * @param accountId 账户ID
     * @return 账户资产列表
     */
    List<AccountAsset> findAccountAssets(Long accountId);

    /**
     * 获取账户指定类型资产
     * 
     * @param accountId 账户ID
     * @param assetType 资产类型
     * @return 账户资产列表
     */
    List<AccountAsset> findAccountAssetsByType(Long accountId, String assetType);

    /**
     * 获取账户转账历史聚合
     * 
     * @param accountId 账户ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 转账历史列表
     */
    List<AccountTransferHistory> findTransferHistory(Long accountId, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取账户在特定代币的转账历史
     * 
     * @param accountId 账户ID
     * @param tokenId 代币ID
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 转账历史列表
     */
    List<AccountTransferHistory> findTransferHistoryByToken(Long accountId, Long tokenId, 
                                                          LocalDateTime startTime, LocalDateTime endTime);
}
