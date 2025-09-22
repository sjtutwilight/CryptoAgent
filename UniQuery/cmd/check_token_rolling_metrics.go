package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/yangguang/Twilight/UniQuery/db"
)

func main() {
	// 初始化数据库连接
	db.InitDB()

	// 检查命令行参数
	if len(os.Args) < 2 {
		log.Fatalf("用法: %s <代币ID>", os.Args[0])
	}

	// 解析代币ID
	tokenID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("解析代币ID失败: %v", err)
	}

	fmt.Printf("检查 token_id = %d 的 token_rolling_metric 数据\n", tokenID)

	// 检查表是否存在
	var tableExists bool
	err = db.DB.Get(&tableExists, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'token_rolling_metric')")
	if err != nil {
		log.Fatalf("检查表是否存在失败: %v", err)
	}

	if !tableExists {
		log.Fatalf("token_rolling_metric 表不存在")
	}

	fmt.Println("token_rolling_metric 表存在")

	// 检查表结构
	var columns []string
	err = db.DB.Select(&columns, "SELECT column_name FROM information_schema.columns WHERE table_name = 'token_rolling_metric'")
	if err != nil {
		log.Fatalf("获取表结构失败: %v", err)
	}

	fmt.Println("表结构:")
	for _, col := range columns {
		fmt.Printf("  - %s\n", col)
	}

	// 检查总记录数
	var totalCount int
	err = db.DB.Get(&totalCount, "SELECT COUNT(*) FROM token_rolling_metric")
	if err != nil {
		log.Fatalf("获取总记录数失败: %v", err)
	}

	fmt.Printf("token_rolling_metric 表总记录数: %d\n", totalCount)

	// 检查特定 token_id 的记录数
	var tokenCount int
	err = db.DB.Get(&tokenCount, "SELECT COUNT(*) FROM token_rolling_metric WHERE token_id = $1", tokenID)
	if err != nil {
		log.Fatalf("获取特定 token_id 的记录数失败: %v", err)
	}

	fmt.Printf("token_id = %d 的记录数: %d\n", tokenID, tokenCount)

	// 如果有记录，获取前 5 条
	if tokenCount > 0 {
		type TokenRollingMetric struct {
			TokenID       int     `db:"token_id"`
			EndTime       string  `db:"end_time"`
			TokenPriceUSD float64 `db:"token_price_usd"`
			MCAP          float64 `db:"mcap"`
		}

		var metrics []TokenRollingMetric
		err = db.DB.Select(&metrics, "SELECT token_id, end_time, token_price_usd, mcap FROM token_rolling_metric WHERE token_id = $1 ORDER BY end_time DESC LIMIT 5", tokenID)
		if err != nil {
			log.Fatalf("获取记录失败: %v", err)
		}

		fmt.Println("\n前 5 条记录:")
		for i, m := range metrics {
			fmt.Printf("  %d. token_id=%d, end_time=%s, price=%f, mcap=%f\n", i+1, m.TokenID, m.EndTime, m.TokenPriceUSD, m.MCAP)
		}
	} else {
		fmt.Println("\n没有找到记录")
	}
}
