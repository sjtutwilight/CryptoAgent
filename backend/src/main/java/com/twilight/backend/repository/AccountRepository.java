package com.twilight.backend.repository;

import com.twilight.backend.model.AccountDetail;

/**
 * 账户数据访问接口
 */
public interface AccountRepository {

    /**
     * 根据ID获取账户详情
     * 
     * @param accountId 账户ID
     * @return 账户详情数据
     */
    AccountDetail findAccountDetailById(Long accountId);
}
