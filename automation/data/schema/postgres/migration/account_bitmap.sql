-- ============================================================
-- 模块: 账户标签位图迁移（Account Tag Bitmap Migration）
-- 存储: PostgreSQL
-- 维护: DataInjector/localnode
-- 用途: 将account表从散列tag字段迁移到位图字段
-- 执行时间: 需要在停止所有应用服务后执行
-- ============================================================

-- ========================================
-- 1. 添加新的位图字段
-- ========================================
ALTER TABLE account ADD COLUMN tag_bitmap INTEGER DEFAULT 0;

-- ========================================
-- 2. 将现有的布尔字段转换为位图值
-- ========================================
UPDATE account SET tag_bitmap = 
    CASE 
        WHEN cex_tag = true THEN tag_bitmap | (1 << 0)  -- EX位
        ELSE tag_bitmap
    END;

UPDATE account SET tag_bitmap = 
    CASE 
        WHEN smart_money_tag = true THEN tag_bitmap | (1 << 1)  -- SM位
        ELSE tag_bitmap
    END;

UPDATE account SET tag_bitmap = 
    CASE 
        WHEN big_whale_tag = true THEN tag_bitmap | (1 << 2)  -- WH位
        ELSE tag_bitmap
    END;

UPDATE account SET tag_bitmap = 
    CASE 
        WHEN fresh_wallet_tag = true THEN tag_bitmap | (1 << 4)  -- FR位
        ELSE tag_bitmap
    END;

-- ========================================
-- 3. 设置新字段为非空
-- ========================================
ALTER TABLE account ALTER COLUMN tag_bitmap SET NOT NULL;

-- ========================================
-- 4. 验证迁移结果
-- ========================================
SELECT 
    address, 
    entity,
    cex_tag, 
    smart_money_tag, 
    big_whale_tag, 
    fresh_wallet_tag,
    tag_bitmap,
    -- 验证位图转换的正确性
    CASE 
        WHEN tag_bitmap & (1 << 0) != 0 THEN 'cex'
        WHEN tag_bitmap & (1 << 1) != 0 THEN 'smart_money'
        WHEN tag_bitmap & (1 << 2) != 0 THEN 'whale'
        WHEN tag_bitmap & (1 << 4) != 0 THEN 'fresh'
        ELSE 'normal'
    END as computed_tag
FROM account
ORDER BY id;

-- ========================================
-- 5. 备份原有字段（可选，用于回滚）
-- ========================================
ALTER TABLE account RENAME COLUMN smart_money_tag TO smart_money_tag_old;
ALTER TABLE account RENAME COLUMN cex_tag TO cex_tag_old;
ALTER TABLE account RENAME COLUMN big_whale_tag TO big_whale_tag_old;
ALTER TABLE account RENAME COLUMN fresh_wallet_tag TO fresh_wallet_tag_old;

-- ========================================
-- 6. 创建索引以提高位图查询性能
-- ========================================
CREATE INDEX idx_account_tag_bitmap ON account(tag_bitmap);

-- ========================================
-- 回滚脚本（如果需要）
-- ========================================
-- ALTER TABLE account RENAME COLUMN smart_money_tag_old TO smart_money_tag;
-- ALTER TABLE account RENAME COLUMN cex_tag_old TO cex_tag;
-- ALTER TABLE account RENAME COLUMN big_whale_tag_old TO big_whale_tag;
-- ALTER TABLE account RENAME COLUMN fresh_wallet_tag_old TO fresh_wallet_tag;
-- ALTER TABLE account DROP COLUMN tag_bitmap;
-- DROP INDEX idx_account_tag_bitmap;






