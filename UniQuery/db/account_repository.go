package db

import (
	"fmt"
	"log"
	"strings"

	"github.com/yangguang/Twilight/UniQuery/models"
)

// GetAccountByID 根据ID获取账户信息
func GetAccountByID(id int) (*models.AccountDB, error) {
	var account models.AccountDB
	err := DB.Get(&account, "SELECT * FROM account WHERE id = $1", id)
	if err != nil {
		log.Printf("获取账户信息失败: %v", err)
		return nil, err
	}
	return &account, nil
}

// GetAccounts 获取账户列表
func GetAccounts(page, limit int) ([]*models.AccountDB, error) {
	offset := (page - 1) * limit
	accounts := []*models.AccountDB{}
	err := DB.Select(&accounts, "SELECT * FROM account ORDER BY id LIMIT $1 OFFSET $2", limit, offset)
	if err != nil {
		log.Printf("获取账户列表失败: %v", err)
		return nil, err
	}
	return accounts, nil
}

// GetAccountAssetsByAccountID 获取账户资产信息
func GetAccountAssetsByAccountID(accountID int, assetType string) ([]*models.AccountAssetViewDB, error) {
	query := "SELECT * FROM account_asset_view WHERE account_id = $1"
	args := []interface{}{accountID}

	if assetType != "" {
		query += " AND asset_type = $2"
		args = append(args, assetType)
	}

	assets := []*models.AccountAssetViewDB{}
	err := DB.Select(&assets, query, args...)
	if err != nil {
		log.Printf("获取账户资产信息失败: %v", err)
		return nil, err
	}
	return assets, nil
}

// GetAccountTransferEvents 获取账户转账事件
func GetAccountTransferEvents(accountID int, page, limit int, buyOrSell, sortBy string) ([]*models.TransferEventDB, error) {
	offset := (page - 1) * limit

	log.Printf("开始获取账户转账事件，账户ID: %d, 页码: %d, 每页数量: %d, 买卖方向: %s, 排序: %s",
		accountID, page, limit, buyOrSell, sortBy)

	// 获取账户地址
	var accountAddress string
	err := DB.Get(&accountAddress, "SELECT address FROM account WHERE id = $1", accountID)
	if err != nil {
		log.Printf("获取账户地址失败: %v", err)
		return nil, err
	}

	log.Printf("获取到账户地址: %s", accountAddress)

	// 首先检查transfer_event表是否存在
	var tableExists bool
	err = DB.Get(&tableExists, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'transfer_event')")
	if err != nil {
		log.Printf("检查表是否存在失败: %v", err)
		return nil, err
	}

	if !tableExists {
		log.Printf("transfer_event表不存在")
		return nil, fmt.Errorf("transfer_event表不存在")
	}

	log.Printf("transfer_event表存在")

	// 检查transfer_event表的列
	var columns []string
	err = DB.Select(&columns, "SELECT column_name FROM information_schema.columns WHERE table_name = 'transfer_event'")
	if err != nil {
		log.Printf("获取表结构失败: %v", err)
		return nil, err
	}

	// 记录列名
	log.Printf("transfer_event表的列: %v", columns)

	// 检查表中是否有数据
	var count int
	err = DB.Get(&count, "SELECT COUNT(*) FROM transfer_event")
	if err != nil {
		log.Printf("获取表数据数量失败: %v", err)
	} else {
		log.Printf("transfer_event表中共有%d条数据", count)
	}

	// 检查特定账户的数据
	var accountEventCount int
	err = DB.Get(&accountEventCount, "SELECT COUNT(*) FROM transfer_event WHERE from_address = $1 OR to_address = $1", accountAddress)
	if err != nil {
		log.Printf("获取账户相关事件数量失败: %v", err)
	} else {
		log.Printf("账户地址 %s 共有 %d 个相关事件", accountAddress, accountEventCount)
	}

	// 如果没有找到事件，尝试使用不区分大小写的查询
	if accountEventCount == 0 {
		log.Printf("尝试使用不区分大小写的查询")
		err = DB.Get(&accountEventCount, "SELECT COUNT(*) FROM transfer_event WHERE LOWER(from_address) = LOWER($1) OR LOWER(to_address) = LOWER($1)", accountAddress)
		if err != nil {
			log.Printf("使用不区分大小写的查询失败: %v", err)
		} else {
			log.Printf("使用不区分大小写的查询，账户地址 %s 共有 %d 个相关事件", accountAddress, accountEventCount)
		}
	}

	// 检查特定买卖方向的数据
	if buyOrSell != "" {
		var directionCount int
		if strings.ToLower(buyOrSell) == "buy" {
			err = DB.Get(&directionCount, "SELECT COUNT(*) FROM transfer_event WHERE to_address = $1", accountAddress)
		} else if strings.ToLower(buyOrSell) == "sell" {
			err = DB.Get(&directionCount, "SELECT COUNT(*) FROM transfer_event WHERE from_address = $1", accountAddress)
		}

		if err != nil {
			log.Printf("获取特定买卖方向事件数量失败: %v", err)
		} else {
			log.Printf("账户地址 %s 的 %s 方向共有 %d 个事件", accountAddress, buyOrSell, directionCount)
		}
	}

	// 构建查询
	selectQuery := `
		SELECT * FROM transfer_event te
	`

	query := selectQuery
	var args []interface{}

	// 添加条件 - 使用不区分大小写的查询
	if buyOrSell != "" {
		if strings.ToLower(buyOrSell) == "buy" {
			// 买入交易：账户地址是接收方
			query += " WHERE LOWER(te.to_address) = LOWER($1)"
			args = append(args, accountAddress)
		} else if strings.ToLower(buyOrSell) == "sell" {
			// 卖出交易：账户地址是发送方
			query += " WHERE LOWER(te.from_address) = LOWER($1)"
			args = append(args, accountAddress)
		}
	} else {
		// 所有交易
		query += " WHERE LOWER(te.from_address) = LOWER($1) OR LOWER(te.to_address) = LOWER($1)"
		args = append(args, accountAddress)
	}

	// 添加排序
	if sortBy == "" {
		sortBy = "te.block_timestamp" // 直接使用transfer_event表的block_timestamp
	} else if sortBy == "block_timestamp" || sortBy == "blocktimestamp" || sortBy == "blockTimestamp" {
		sortBy = "te.block_timestamp" // 直接使用transfer_event表的block_timestamp
	} else if sortBy == "value" || sortBy == "valueUsd" || sortBy == "valueUSD" {
		sortBy = "te.value_usd"
	}

	// 记录排序字段
	log.Printf("排序字段: %s", sortBy)

	query += fmt.Sprintf(" ORDER BY %s DESC", sortBy)

	// 添加分页
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
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
		log.Printf("获取账户转账事件失败: %v", err)
		return nil, err
	}

	log.Printf("返回%d个账户转账事件", len(events))

	// 打印每个事件的详细信息
	for i, event := range events {
		log.Printf("事件 %d: ID=%d, 发送方=%s, 接收方=%s, 代币=%s, 价值=%f",
			i, event.ID, event.FromAddress, event.ToAddress, event.TokenSymbol, event.ValueUSD)
	}

	return events, nil
}

// GetAccountTransferHistory 获取账户转账历史
func GetAccountTransferHistory(accountID int, page, limit int, buyOrSell, sortBy string) ([]*models.AccountTransferHistoryDB, error) {
	offset := (page - 1) * limit
	query := "SELECT * FROM account_transfer_history WHERE account_id = $1"
	args := []interface{}{accountID}
	argCount := 1

	// 添加买卖筛选条件
	if buyOrSell != "" {
		if strings.ToLower(buyOrSell) == "buy" {
			query += fmt.Sprintf(" AND isBuy = $%d", argCount+1)
			args = append(args, 1)
			argCount++
		} else if strings.ToLower(buyOrSell) == "sell" {
			query += fmt.Sprintf(" AND isBuy = $%d", argCount+1)
			args = append(args, 0)
			argCount++
		}
	}

	// 添加排序
	if sortBy == "" {
		sortBy = "end_time"
	}
	query += fmt.Sprintf(" ORDER BY %s DESC", sortBy)

	// 添加分页
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argCount+1, argCount+2)
	args = append(args, limit, offset)

	history := []*models.AccountTransferHistoryDB{}
	err := DB.Select(&history, query, args...)
	if err != nil {
		log.Printf("获取账户转账历史失败: %v", err)
		return nil, err
	}
	return history, nil
}
