package db

import (
	"fmt"
	"log"
	"strings"

	"github.com/yangguang/Twilight/UniQuery/models"
)

// GetTokenByID 根据ID获取代币信息
func GetTokenByID(id int) (*models.TokenDB, error) {
	var token models.TokenDB
	err := DB.Get(&token, "SELECT * FROM token WHERE id = $1", id)
	if err != nil {
		log.Printf("获取代币信息失败: %v", err)
		return nil, err
	}
	return &token, nil
}

// GetTokens 获取代币列表
func GetTokens(page, limit int, sortBy string) ([]*models.TokenDB, error) {
	offset := (page - 1) * limit

	// 默认排序字段
	if sortBy == "" {
		sortBy = "id"
	}

	// 如果排序字段是mcap，需要关联token_metric表
	var query string
	if sortBy == "mcap" {
		query = `
			SELECT t.* 
			FROM token t
			JOIN token_metric tm ON t.id = tm.token_id
			ORDER BY tm.mcap DESC
			LIMIT $1 OFFSET $2
		`
	} else {
		query = fmt.Sprintf("SELECT * FROM token ORDER BY %s LIMIT $1 OFFSET $2", sortBy)
	}

	tokens := []*models.TokenDB{}
	err := DB.Select(&tokens, query, limit, offset)
	if err != nil {
		log.Printf("获取代币列表失败: %v", err)
		return nil, err
	}
	return tokens, nil
}

// GetTokenMetricByTokenID 获取代币指标
func GetTokenMetricByTokenID(tokenID int) (*models.TokenMetricDB, error) {
	var metric models.TokenMetricDB
	err := DB.Get(&metric, "SELECT * FROM token_metric WHERE token_id = $1", tokenID)
	if err != nil {
		log.Printf("获取代币指标失败: %v", err)
		return nil, err
	}
	return &metric, nil
}

// GetTokenRecentMetricByTokenID 获取代币最近指标
func GetTokenRecentMetricByTokenID(tokenID int, timeWindow, tag string) (*models.TokenRecentMetricDB, error) {
	if tag == "" {
		tag = "all" // 默认获取所有标签的指标
	}

	var metric models.TokenRecentMetricDB
	err := DB.Get(&metric, `
		SELECT * FROM token_recent_metric 
		WHERE token_id = $1 AND time_window = $2 AND tag = $3
		ORDER BY end_time DESC
		LIMIT 1
	`, tokenID, timeWindow, tag)
	if err != nil {
		log.Printf("获取代币最近指标失败: %v", err)
		return nil, err
	}
	return &metric, nil
}

// GetTokenRollingMetrics 获取代币滚动指标
func GetTokenRollingMetrics(tokenID int, limit int, startTime, endTime string) ([]*models.TokenRollingMetricDB, error) {
	log.Printf("开始获取代币滚动指标，代币ID: %d, 限制: %d, 开始时间: %s, 结束时间: %s", tokenID, limit, startTime, endTime)

	// 检查表是否存在
	var tableExists bool
	err := DB.Get(&tableExists, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'token_rolling_metric')")
	if err != nil {
		log.Printf("检查表是否存在失败: %v", err)
		return nil, err
	}

	if !tableExists {
		log.Printf("token_rolling_metric表不存在")
		return nil, fmt.Errorf("token_rolling_metric表不存在")
	}

	log.Printf("token_rolling_metric表存在")

	// 检查表结构
	var columns []string
	err = DB.Select(&columns, "SELECT column_name FROM information_schema.columns WHERE table_name = 'token_rolling_metric'")
	if err != nil {
		log.Printf("获取表结构失败: %v", err)
		return nil, err
	}

	log.Printf("token_rolling_metric表的列: %v", columns)

	// 检查表中是否有数据
	var count int
	err = DB.Get(&count, "SELECT COUNT(*) FROM token_rolling_metric")
	if err != nil {
		log.Printf("获取表数据数量失败: %v", err)
	} else {
		log.Printf("token_rolling_metric表中共有%d条数据", count)
	}

	// 检查特定token_id的数据
	err = DB.Get(&count, "SELECT COUNT(*) FROM token_rolling_metric WHERE token_id = $1", tokenID)
	if err != nil {
		log.Printf("获取token_id=%d的数据数量失败: %v", tokenID, err)
	} else {
		log.Printf("token_id=%d共有%d条滚动指标数据", tokenID, count)
	}

	// 直接查询，不使用 time_window 字段
	query := "SELECT token_id, end_time, token_price_usd, mcap FROM token_rolling_metric WHERE token_id = $1"
	args := []interface{}{tokenID}
	argCount := 1

	// 添加时间范围筛选
	if startTime != "" {
		query += fmt.Sprintf(" AND end_time >= $%d", argCount+1)
		args = append(args, startTime)
		argCount++
	}

	if endTime != "" {
		query += fmt.Sprintf(" AND end_time <= $%d", argCount+1)
		args = append(args, endTime)
		argCount++
	}

	// 添加排序和分页
	query += " ORDER BY end_time DESC"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argCount+1)
		args = append(args, limit)
	}

	log.Printf("SQL查询: %s, 参数: %v", query, args)

	metrics := []*models.TokenRollingMetricDB{}
	err = DB.Select(&metrics, query, args...)
	if err != nil {
		log.Printf("获取代币滚动指标失败: %v", err)
		return nil, err
	}

	log.Printf("返回%d个代币滚动指标", len(metrics))
	for i, m := range metrics {
		log.Printf("指标 %d: token_id=%d, end_time=%v, price=%f, mcap=%f",
			i+1, m.TokenID, m.EndTime, m.TokenPriceUSD, m.MCAP)
	}

	return metrics, nil
}

// GetTokenTransferEvents 获取代币转账事件
func GetTokenTransferEvents(tokenID int, page, limit int, buyOrSell string, onlySmartMoney, onlyWithCex, onlyWithDex bool, sortBy string) ([]*models.TransferEventDB, error) {
	offset := (page - 1) * limit

	log.Printf("开始获取代币转账事件，代币ID: %d, 页码: %d, 每页数量: %d, 买卖方向: %s, 排序: %s",
		tokenID, page, limit, buyOrSell, sortBy)

	// 首先检查transfer_event表是否存在
	var tableExists bool
	err := DB.Get(&tableExists, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'transfer_event')")
	if err != nil {
		log.Printf("检查表是否存在失败: %v", err)
		return nil, err
	}

	if !tableExists {
		log.Printf("transfer_event表不存在")
		return nil, fmt.Errorf("transfer_event表不存在")
	}

	// 检查transfer_event表的列
	var columns []string
	err = DB.Select(&columns, "SELECT column_name FROM information_schema.columns WHERE table_name = 'transfer_event'")
	if err != nil {
		log.Printf("获取表结构失败: %v", err)
		return nil, err
	}

	// 记录列名
	log.Printf("transfer_event表的列: %v", columns)

	// 检查是否有block_timestamp列
	hasBlockTimestamp := false
	for _, col := range columns {
		if col == "block_timestamp" {
			hasBlockTimestamp = true
			break
		}
	}

	// 构建查询
	selectQuery := "SELECT * FROM transfer_event te"
	query := selectQuery + " WHERE te.token_id = $1"
	args := []interface{}{tokenID}
	argCount := 1

	// 添加买卖筛选条件
	if buyOrSell != "" {
		// 买入：to_address是接收方，卖出：from_address是发送方
		if strings.ToLower(buyOrSell) == "buy" {
			// 买入交易：接收方接收代币
			query += " AND EXISTS (SELECT 1 FROM account a WHERE LOWER(a.address) = LOWER(te.to_address))"
		} else if strings.ToLower(buyOrSell) == "sell" {
			// 卖出交易：发送方发送代币
			query += " AND EXISTS (SELECT 1 FROM account a WHERE LOWER(a.address) = LOWER(te.from_address))"
		}
	}

	// 添加智能钱包筛选
	if onlySmartMoney {
		query += `
			AND (
				EXISTS (SELECT 1 FROM account a WHERE LOWER(a.address) = LOWER(te.from_address) AND a.smart_money_tag = true)
				OR EXISTS (SELECT 1 FROM account a WHERE LOWER(a.address) = LOWER(te.to_address) AND a.smart_money_tag = true)
			)
		`
	}

	// 添加交易所筛选
	if onlyWithCex {
		query += `
			AND (
				EXISTS (SELECT 1 FROM account a WHERE LOWER(a.address) = LOWER(te.from_address) AND a.cex_tag = true)
				OR EXISTS (SELECT 1 FROM account a WHERE LOWER(a.address) = LOWER(te.to_address) AND a.cex_tag = true)
			)
		`
	}

	// 添加DEX筛选（这里简化处理，实际可能需要更复杂的逻辑）
	if onlyWithDex {
		query += `
			AND (
				EXISTS (SELECT 1 FROM twswap_pair p WHERE LOWER(p.pair_address) = LOWER(te.from_address))
				OR EXISTS (SELECT 1 FROM twswap_pair p WHERE LOWER(p.pair_address) = LOWER(te.to_address))
			)
		`
	}

	// 添加排序
	if sortBy == "" {
		if hasBlockTimestamp {
			sortBy = "te.block_timestamp"
		} else {
			// 如果没有block_timestamp列，使用id排序
			sortBy = "te.id"
		}
	} else if sortBy == "block_timestamp" || sortBy == "blocktimestamp" || sortBy == "blockTimestamp" {
		if hasBlockTimestamp {
			sortBy = "te.block_timestamp"
		} else {
			// 如果没有block_timestamp列，使用id排序
			sortBy = "te.id"
		}
	} else if sortBy == "value" {
		sortBy = "te.value_usd"
	} else if sortBy == "valueUsd" || sortBy == "valueUSD" {
		sortBy = "te.value_usd"
	}

	// 记录排序字段
	log.Printf("排序字段: %s", sortBy)

	query += fmt.Sprintf(" ORDER BY %s DESC", sortBy)

	// 添加分页
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argCount+1, argCount+2)
	args = append(args, limit, offset)

	// 记录最终查询
	log.Printf("最终查询: %s, 参数: %v", query, args)

	// 尝试直接执行查询，不使用sqlx
	rows, err := DB.DB.Query(query, args...)
	if err != nil {
		log.Printf("直接执行查询失败: %v", err)
	} else {
		defer rows.Close()

		// 获取列名
		cols, err := rows.Columns()
		if err != nil {
			log.Printf("获取列名失败: %v", err)
		} else {
			log.Printf("查询结果列名: %v", cols)
		}

		// 计算行数
		rowCount := 0
		for rows.Next() {
			rowCount++
		}
		log.Printf("查询结果行数: %d", rowCount)

		// 检查是否有错误
		if err := rows.Err(); err != nil {
			log.Printf("遍历结果时出错: %v", err)
		}
	}

	events := []*models.TransferEventDB{}
	err = DB.Select(&events, query, args...)
	if err != nil {
		log.Printf("获取代币转账事件失败: %v", err)
		return nil, err
	}

	log.Printf("返回%d个代币转账事件", len(events))

	// 打印每个事件的详细信息
	for i, event := range events {
		log.Printf("事件 %d: ID=%d, 发送方=%s, 接收方=%s, 代币ID=%d, 价值=%f",
			i, event.ID, event.FromAddress, event.ToAddress, event.TokenID, event.ValueUSD)
	}

	return events, nil
}

// GetTokenHolders 获取代币持有者
func GetTokenHolders(tokenID int, page, limit int, sortBy string) ([]*models.TokenHolderDB, error) {
	offset := (page - 1) * limit

	log.Printf("开始获取代币持有者，代币ID: %d (类型: %T), 页码: %d, 每页数量: %d, 排序: %s",
		tokenID, tokenID, page, limit, sortBy)

	// 检查token_holder表是否存在
	var tableExists bool
	err := DB.Get(&tableExists, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'token_holder')")
	if err != nil {
		log.Printf("检查表是否存在失败: %v", err)
		return nil, err
	}

	if !tableExists {
		log.Printf("token_holder表不存在")
		return []*models.TokenHolderDB{}, nil
	}

	log.Printf("token_holder表存在")

	// 检查表中是否有数据
	var count int
	err = DB.Get(&count, "SELECT COUNT(*) FROM token_holder")
	if err != nil {
		log.Printf("获取表数据数量失败: %v", err)
	} else {
		log.Printf("token_holder表中共有%d条数据", count)
	}

	// 检查表中是否有特定token_id的数据
	err = DB.Get(&count, "SELECT COUNT(*) FROM token_holder WHERE token_id = $1", tokenID)
	if err != nil {
		log.Printf("获取token_id=%d的数据数量失败: %v", tokenID, err)
	} else {
		log.Printf("token_id=%d共有%d个持有者", tokenID, count)
	}

	// 获取所有不同的token_id
	var tokenIDs []int
	err = DB.Select(&tokenIDs, "SELECT DISTINCT token_id FROM token_holder")
	if err != nil {
		log.Printf("获取不同token_id失败: %v", err)
	} else {
		log.Printf("token_holder表中共有%d个不同的token_id: %v", len(tokenIDs), tokenIDs)
	}

	// 获取前几个持有者的详细信息
	if count > 0 {
		var topHolders []struct {
			TokenID        int     `db:"token_id"`
			AccountID      int     `db:"account_id"`
			AccountAddress string  `db:"account_address"`
			Ownership      float64 `db:"ownership"`
			ValueUSD       float64 `db:"value_usd"`
		}
		err = DB.Select(&topHolders, "SELECT token_id, account_id, account_address, ownership, value_usd FROM token_holder WHERE token_id = $1 ORDER BY ownership DESC LIMIT 3", tokenID)
		if err != nil {
			log.Printf("获取token_id=%d的前几个持有者失败: %v", tokenID, err)
		} else {
			for i, holder := range topHolders {
				log.Printf("token_id=%d的第%d个持有者: account_id=%d, address=%s, ownership=%f, value_usd=%f",
					tokenID, i+1, holder.AccountID, holder.AccountAddress, holder.Ownership, holder.ValueUSD)
			}
		}
	}

	// 检查token_holder表的列
	var columns []string
	err = DB.Select(&columns, "SELECT column_name FROM information_schema.columns WHERE table_name = 'token_holder'")
	if err != nil {
		log.Printf("获取表结构失败: %v", err)
		return nil, err
	}

	// 记录列名
	log.Printf("token_holder表的列: %v", columns)

	// 检查是否有token_id列
	hasTokenID := false
	for _, col := range columns {
		if col == "token_id" {
			hasTokenID = true
			break
		}
	}

	if !hasTokenID {
		log.Printf("token_holder表没有token_id列")
		return []*models.TokenHolderDB{}, nil
	}

	// 检查排序字段是否存在
	sortFieldExists := false
	for _, col := range columns {
		if col == sortBy {
			sortFieldExists = true
			break
		}
	}

	if !sortFieldExists {
		log.Printf("排序字段%s不存在，使用默认排序", sortBy)
		sortBy = "ownership"

		// 再次检查ownership字段是否存在
		ownershipExists := false
		for _, col := range columns {
			if col == "ownership" {
				ownershipExists = true
				break
			}
		}

		if !ownershipExists {
			log.Printf("ownership字段也不存在，使用第一个列作为排序字段")
			if len(columns) > 0 {
				sortBy = columns[0]
			} else {
				log.Printf("表没有列，无法排序")
				return []*models.TokenHolderDB{}, nil
			}
		}
	}

	// 使用参数绑定的查询
	query := fmt.Sprintf("SELECT * FROM token_holder WHERE token_id = $1 ORDER BY %s DESC LIMIT $2 OFFSET $3", sortBy)
	log.Printf("参数绑定查询: %s, 参数: %v, %v, %v", query, tokenID, limit, offset)

	holders := []*models.TokenHolderDB{}
	err = DB.Select(&holders, query, tokenID, limit, offset)
	if err != nil {
		log.Printf("获取代币持有者失败: %v", err)
		return nil, err
	}

	log.Printf("获取到%d个代币持有者", len(holders))

	// 打印每个持有者的信息
	for i, holder := range holders {
		log.Printf("持有者 %d: 账户ID=%d, 账户地址=%s, 所有权=%f, 价值=%f",
			i, holder.AccountID, holder.AccountAddress, holder.Ownership, holder.ValueUSD)
	}

	return holders, nil
}

// GetPairAddressByID 根据ID获取交易对地址
func GetPairAddressByID(id int) (string, error) {
	// 首先检查twswap_pair表是否存在
	var tableExists bool
	err := DB.Get(&tableExists, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'twswap_pair')")
	if err != nil {
		log.Printf("检查表是否存在失败: %v", err)
		return "", err
	}

	if tableExists {
		// 将int转换为int64
		id64 := int64(id)

		// 尝试从twswap_pair表获取
		var pairAddress string
		err = DB.Get(&pairAddress, "SELECT pair_address FROM twswap_pair WHERE id = $1", id64)
		if err == nil && pairAddress != "" {
			return pairAddress, nil
		}

		// 如果失败，尝试使用不同的列名
		err = DB.Get(&pairAddress, "SELECT address FROM twswap_pair WHERE id = $1", id64)
		if err == nil && pairAddress != "" {
			return pairAddress, nil
		}
	}

	// 如果twswap_pair表不存在或查询失败，尝试从pair表获取
	var pairExists bool
	err = DB.Get(&pairExists, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'pair')")
	if err != nil {
		log.Printf("检查pair表是否存在失败: %v", err)
		return "", err
	}

	if pairExists {
		var pairAddress string
		err = DB.Get(&pairAddress, "SELECT pair_address FROM pair WHERE id = $1", id)
		if err == nil && pairAddress != "" {
			return pairAddress, nil
		}

		// 如果失败，尝试使用不同的列名
		err = DB.Get(&pairAddress, "SELECT address FROM pair WHERE id = $1", id)
		if err == nil && pairAddress != "" {
			return pairAddress, nil
		}
	}

	// 如果所有尝试都失败，返回空字符串
	log.Printf("无法找到ID为 %d 的交易对地址", id)
	return "", nil
}

// CheckTokenHolderData 检查token_holder表中是否有数据
func CheckTokenHolderData() error {
	// 检查token_holder表是否存在
	var tableExists bool
	err := DB.Get(&tableExists, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'token_holder')")
	if err != nil {
		log.Printf("检查表是否存在失败: %v", err)
		return err
	}

	if !tableExists {
		log.Printf("token_holder表不存在")
		return fmt.Errorf("token_holder表不存在")
	}

	log.Printf("token_holder表存在")

	// 检查表中是否有数据
	var count int
	err = DB.Get(&count, "SELECT COUNT(*) FROM token_holder")
	if err != nil {
		log.Printf("获取表数据数量失败: %v", err)
		return err
	}

	log.Printf("token_holder表中共有%d条数据", count)

	// 获取所有token_id
	var tokenIDs []int
	err = DB.Select(&tokenIDs, "SELECT DISTINCT token_id FROM token_holder")
	if err != nil {
		log.Printf("获取token_id失败: %v", err)
		return err
	}

	log.Printf("token_holder表中共有%d个不同的token_id: %v", len(tokenIDs), tokenIDs)

	// 获取每个token_id的持有者数量
	for _, tokenID := range tokenIDs {
		var holderCount int
		err = DB.Get(&holderCount, "SELECT COUNT(*) FROM token_holder WHERE token_id = $1", tokenID)
		if err != nil {
			log.Printf("获取token_id=%d的持有者数量失败: %v", tokenID, err)
			continue
		}

		log.Printf("token_id=%d共有%d个持有者", tokenID, holderCount)

		// 获取该token_id的前3个持有者
		var holders []struct {
			TokenID        int     `db:"token_id"`
			AccountID      int     `db:"account_id"`
			AccountAddress string  `db:"account_address"`
			Ownership      float64 `db:"ownership"`
			ValueUSD       float64 `db:"value_usd"`
		}
		err = DB.Select(&holders, "SELECT token_id, account_id, account_address, ownership, value_usd FROM token_holder WHERE token_id = $1 ORDER BY ownership DESC LIMIT 3", tokenID)
		if err != nil {
			log.Printf("获取token_id=%d的持有者失败: %v", tokenID, err)
			continue
		}

		for i, holder := range holders {
			log.Printf("token_id=%d的第%d个持有者: account_id=%d, address=%s, ownership=%f, value_usd=%f",
				tokenID, i+1, holder.AccountID, holder.AccountAddress, holder.Ownership, holder.ValueUSD)
		}
	}

	return nil
}
