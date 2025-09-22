package resolvers

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"time"

	"github.com/yangguang/Twilight/UniQuery/db"
	"github.com/yangguang/Twilight/UniQuery/graph/generated"
	"github.com/yangguang/Twilight/UniQuery/models"
)

// RootResolver 是根解析器
type RootResolver struct{}

// NewRootResolver 创建根解析器
func NewRootResolver() *RootResolver {
	return &RootResolver{}
}

// Query 返回查询解析器
func (r *RootResolver) Query() generated.QueryResolver {
	return &QueryResolver{}
}

// QueryResolver 是查询解析器
type QueryResolver struct{}

// NewQueryResolver 创建查询解析器
func NewQueryResolver() *QueryResolver {
	return &QueryResolver{}
}

// Account 解析账户查询
func (r *QueryResolver) Account(ctx context.Context, id string) (*models.Account, error) {
	// 将ID转换为整数
	accountID, err := strconv.Atoi(id)
	if err != nil {
		log.Printf("账户ID格式错误: %v", err)
		return nil, err
	}

	// 获取账户信息
	account, err := db.GetAccountByID(accountID)
	if err != nil {
		log.Printf("获取账户信息失败: %v", err)
		return nil, err
	}

	// 创建账户解析器
	resolver := NewAccountResolver(account)

	// 获取账户资产信息
	ethBalance := resolver.EthBalance()

	// 获取ERC20余额
	erc20Resolvers := resolver.ERC20Balances()
	erc20Balances := make([]*models.ERC20Balance, len(erc20Resolvers))
	for i, r := range erc20Resolvers {
		erc20Balances[i] = &models.ERC20Balance{
			TokenAddress: r.TokenAddress(),
			TokenSymbol:  r.TokenSymbol(),
			Balance:      r.Balance(),
			Price:        r.Price(),
			ValueUsd:     r.ValueUSD(),
		}
	}

	// 获取DeFi持仓
	defiResolvers := resolver.DefiPositions()
	defiPositions := make([]*models.DefiPosition, len(defiResolvers))
	for i, r := range defiResolvers {
		defiPositions[i] = &models.DefiPosition{
			Protocol:        r.Protocol(),
			ContractAddress: r.ContractAddress(),
			Position:        r.Position(),
			ValueUsd:        r.ValueUSD(),
		}
	}

	// 获取标签
	labels := resolver.Labels()

	// 转换为GraphQL模型
	return &models.Account{
		ID:            id,
		ChainName:     account.ChainName,
		Address:       account.Address,
		Entity:        &account.Entity,
		Labels:        labels,
		EthBalance:    ethBalance,
		Erc20Balances: erc20Balances,
		DefiPositions: defiPositions,
	}, nil
}

// Accounts 解析账户列表查询
func (r *QueryResolver) Accounts(ctx context.Context, page *int, limit *int) ([]*models.Account, error) {
	// 设置默认值
	p := 1
	if page != nil {
		p = *page
	}

	l := 10
	if limit != nil {
		l = *limit
	}

	// 获取账户列表
	accounts, err := db.GetAccounts(p, l)
	if err != nil {
		log.Printf("获取账户列表失败: %v", err)
		return nil, err
	}

	// 转换为GraphQL模型
	result := make([]*models.Account, len(accounts))
	for i, account := range accounts {
		id := strconv.Itoa(account.ID)

		// 生成标签列表
		labels := []string{}
		if account.SmartMoneyTag {
			labels = append(labels, "聪明钱")
		}
		if account.CexTag {
			labels = append(labels, "交易所")
		}
		if account.BigWhaleTag {
			labels = append(labels, "巨鲸")
		}
		if account.FreshWalletTag {
			labels = append(labels, "fresh account")
		}

		result[i] = &models.Account{
			ID:        id,
			ChainName: account.ChainName,
			Address:   account.Address,
			Entity:    &account.Entity,
			Labels:    labels,
		}
	}

	return result, nil
}

// AccountTransferEvents 查询账户转账事件
func (r *QueryResolver) AccountTransferEvents(ctx context.Context, accountID string, page *int, limit *int, buyOrSell *string, sortBy *string) ([]*models.AccountTransferEvent, error) {
	// 解析账户ID
	accountIDInt, err := strconv.Atoi(accountID)
	if err != nil {
		log.Printf("解析账户ID失败: %v", err)
		return nil, err
	}

	// 设置默认值
	pageVal := 1
	if page != nil {
		pageVal = *page
	}

	limitVal := 10
	if limit != nil {
		limitVal = *limit
	}

	buyOrSellVal := ""
	if buyOrSell != nil {
		buyOrSellVal = *buyOrSell
	}

	sortByVal := "blockTimestamp"
	if sortBy != nil {
		sortByVal = *sortBy
	}

	// 获取账户转账事件
	events, err := db.GetAccountTransferEvents(accountIDInt, pageVal, limitVal, buyOrSellVal, sortByVal)
	if err != nil {
		log.Printf("获取账户转账事件失败: %v", err)
		return nil, err
	}

	// 创建解析器
	resolvers := make([]*AccountTransferEventResolver, len(events))
	for i, event := range events {
		resolvers[i] = &AccountTransferEventResolver{
			event:     event,
			accountId: accountIDInt,
		}
	}

	// 转换为GraphQL模型
	result := make([]*models.AccountTransferEvent, len(resolvers))
	for i, resolver := range resolvers {
		result[i] = &models.AccountTransferEvent{
			AccountID:      resolver.AccountId(),
			BlockTimestamp: resolver.BlockTimestamp(),
			FromAddress:    resolver.FromAddress(),
			BuyOrSell:      resolver.BuyOrSell(),
			ToAddress:      resolver.ToAddress(),
			TokenSymbol:    resolver.TokenSymbol(),
			ValueUsd:       resolver.ValueUSD(),
		}
	}

	return result, nil
}

// AccountTransferHistory 解析账户转账历史查询
func (r *QueryResolver) AccountTransferHistory(ctx context.Context, accountId string, page *int, limit *int, buyOrSell *string, sortBy *string) ([]*models.AccountTransferHistory, error) {
	// 将ID转换为整数
	id, err := strconv.Atoi(accountId)
	if err != nil {
		log.Printf("账户ID格式错误: %v", err)
		return nil, err
	}

	// 设置默认值
	p := 1
	if page != nil {
		p = *page
	}

	l := 10
	if limit != nil {
		l = *limit
	}

	var b string
	if buyOrSell != nil {
		b = *buyOrSell
	}

	var s string
	if sortBy != nil {
		s = *sortBy
	}

	// 获取转账历史
	history, err := db.GetAccountTransferHistory(id, p, l, b, s)
	if err != nil {
		log.Printf("获取账户转账历史失败: %v", err)
		return nil, err
	}

	// 转换为GraphQL模型
	result := make([]*models.AccountTransferHistory, len(history))
	for i, h := range history {
		// 判断买入还是卖出
		var buyOrSell string
		if h.IsBuy == 1 {
			buyOrSell = "buy"
		} else {
			buyOrSell = "sell"
		}

		result[i] = &models.AccountTransferHistory{
			EndTime:       formatTime(h.EndTime),
			BuyOrSell:     buyOrSell,
			TxCnt:         h.TxCnt,
			TotalValueUsd: h.TotalValueUSD,
		}
	}

	return result, nil
}

// Token 解析代币查询
func (r *QueryResolver) Token(ctx context.Context, id string) (*models.Token, error) {
	log.Printf("开始查询代币，ID: %s", id)

	// 将ID转换为整数
	tokenID, err := strconv.Atoi(id)
	if err != nil {
		log.Printf("代币ID格式错误: %v", err)
		return nil, err
	}

	log.Printf("转换后的代币ID: %d (类型: %T)", tokenID, tokenID)

	// 获取代币信息
	token, err := db.GetTokenByID(tokenID)
	if err != nil {
		log.Printf("获取代币信息失败: %v", err)
		return nil, err
	}

	log.Printf("获取到代币信息: ID=%d, 符号=%s, 地址=%s", token.ID, token.TokenSymbol, token.TokenAddress)

	// 获取代币指标
	metric, err := db.GetTokenMetricByTokenID(tokenID)
	if err != nil {
		log.Printf("获取代币指标失败: %v", err)
		// 继续执行，不返回错误
	} else if metric != nil {
		log.Printf("获取到代币指标: 价格=%f, 市值=%f, 流动性=%f", metric.TokenPrice, metric.MCAP, metric.LiquidityUSD)
	}

	// 创建TokenDetail
	var tokenDetail *models.TokenDetail
	if metric != nil {
		// 处理TokenAge
		tokenAgeStr := "0天"
		if metric.TokenAge > 0 {
			tokenAgeStr = strconv.Itoa(metric.TokenAge) + "天"
		}

		// 获取最近指标
		tokenRecentMetric := createTokenRecentMetric(tokenID, "1min")

		tokenDetail = &models.TokenDetail{
			ID:            id,
			ChainName:     token.ChainName,
			TokenSymbol:   token.TokenSymbol,
			Price:         metric.TokenPrice,
			Mcap:          metric.MCAP,
			Liquidity:     metric.LiquidityUSD,
			Fdv:           metric.FDV,
			Issuer:        token.Issuer,
			TokenAge:      tokenAgeStr,
			TokenCatagory: token.TokenCatagory,
			SecurityScore: float64(metric.SecurityScore),
			RecentMetrics: tokenRecentMetric,
		}

		log.Printf("创建了TokenDetail对象: ID=%s, 符号=%s", tokenDetail.ID, tokenDetail.TokenSymbol)
	} else {
		log.Printf("警告: 没有创建TokenDetail对象，因为metric为nil")
	}

	// 转换为GraphQL模型
	result := &models.Token{
		ID:           id,
		ChainName:    token.ChainName,
		TokenSymbol:  token.TokenSymbol,
		TokenAddress: token.TokenAddress,
		TokenDetail:  tokenDetail,
	}

	log.Printf("返回Token对象: ID=%s, 符号=%s, 是否有TokenDetail=%v",
		result.ID, result.TokenSymbol, result.TokenDetail != nil)

	return result, nil
}

// Tokens 解析代币列表查询
func (r *QueryResolver) Tokens(ctx context.Context, page *int, limit *int, sortBy *string) ([]*models.Token, error) {
	// 设置默认值
	p := 1
	if page != nil {
		p = *page
	}

	l := 10
	if limit != nil {
		l = *limit
	}

	var s string
	if sortBy != nil {
		s = *sortBy
	} else {
		s = "mcap" // 默认按市值排序
	}

	// 获取代币列表
	tokens, err := db.GetTokens(p, l, s)
	if err != nil {
		log.Printf("获取代币列表失败: %v", err)
		return nil, err
	}

	// 转换为GraphQL模型
	result := make([]*models.Token, len(tokens))
	for i, token := range tokens {
		id := strconv.Itoa(token.ID)

		// 获取代币指标
		metric, err := db.GetTokenMetricByTokenID(token.ID)
		if err != nil {
			log.Printf("获取代币指标失败: %v", err)
			// 继续执行，不返回错误
		}

		// 创建TokenDetail
		var tokenDetail *models.TokenDetail
		if metric != nil {
			// 处理TokenAge
			tokenAgeStr := "0天"
			if metric.TokenAge > 0 {
				tokenAgeStr = strconv.Itoa(metric.TokenAge) + "天"
			}

			// 获取最近指标
			tokenRecentMetric := createTokenRecentMetric(token.ID, "1min")

			tokenDetail = &models.TokenDetail{
				ID:            id,
				ChainName:     token.ChainName,
				TokenSymbol:   token.TokenSymbol,
				Price:         metric.TokenPrice,
				Mcap:          metric.MCAP,
				Liquidity:     metric.LiquidityUSD,
				Fdv:           metric.FDV,
				Issuer:        token.Issuer,
				TokenAge:      tokenAgeStr,
				TokenCatagory: token.TokenCatagory,
				SecurityScore: float64(metric.SecurityScore),
				RecentMetrics: tokenRecentMetric,
			}
		}

		result[i] = &models.Token{
			ID:           id,
			ChainName:    token.ChainName,
			TokenSymbol:  token.TokenSymbol,
			TokenAddress: token.TokenAddress,
			TokenDetail:  tokenDetail,
		}
	}

	return result, nil
}

// TokenTransferEvents 查询代币转账事件
func (r *QueryResolver) TokenTransferEvents(ctx context.Context, tokenId string, page *int, limit *int, buyOrSell *string, onlySmartMoney *bool, onlyWithCex *bool, onlyWithDex *bool, sortBy *string) ([]*models.TokenTransferEvent, error) {
	// 解析代币ID
	tokenIDInt, err := strconv.Atoi(tokenId)
	if err != nil {
		log.Printf("解析代币ID失败: %v", err)
		return nil, err
	}

	// 设置默认值
	pageVal := 1
	if page != nil {
		pageVal = *page
	}

	limitVal := 10
	if limit != nil {
		limitVal = *limit
	}

	buyOrSellVal := ""
	if buyOrSell != nil {
		buyOrSellVal = *buyOrSell
	}

	onlySmartMoneyVal := false
	if onlySmartMoney != nil {
		onlySmartMoneyVal = *onlySmartMoney
	}

	onlyWithCexVal := false
	if onlyWithCex != nil {
		onlyWithCexVal = *onlyWithCex
	}

	onlyWithDexVal := false
	if onlyWithDex != nil {
		onlyWithDexVal = *onlyWithDex
	}

	sortByVal := "block_timestamp"
	if sortBy != nil {
		sortByVal = *sortBy
		// 转换GraphQL字段名到数据库字段名
		if sortByVal == "blockTimestamp" {
			sortByVal = "block_timestamp"
		} else if sortByVal == "valueUSD" {
			sortByVal = "value_usd"
		}
	}

	// 获取代币转账事件
	events, err := db.GetTokenTransferEvents(tokenIDInt, pageVal, limitVal, buyOrSellVal, onlySmartMoneyVal, onlyWithCexVal, onlyWithDexVal, sortByVal)
	if err != nil {
		log.Printf("获取代币转账事件失败: %v", err)
		return nil, err
	}

	// 创建解析器
	resolvers := make([]*TokenTransferEventResolver, len(events))
	for i, event := range events {
		resolvers[i] = &TokenTransferEventResolver{
			event:   event,
			tokenId: tokenIDInt,
		}
	}

	// 转换为GraphQL模型
	result := make([]*models.TokenTransferEvent, len(resolvers))
	for i, resolver := range resolvers {
		result[i] = &models.TokenTransferEvent{
			TokenID:        resolver.TokenId(),
			FromAddress:    resolver.FromAddress(),
			ToAddress:      resolver.ToAddress(),
			ValueUsd:       resolver.ValueUSD(),
			BlockTimestamp: resolver.BlockTimestamp(),
		}
	}

	return result, nil
}

// TokenHolders 查询代币持有者
func (r *QueryResolver) TokenHolders(ctx context.Context, tokenId string, page *int, limit *int, sortBy *string) ([]*models.TokenHolder, error) {
	// 解析代币ID
	tokenIDInt, err := strconv.Atoi(tokenId)
	if err != nil {
		log.Printf("解析代币ID失败: %v", err)
		return nil, err
	}

	// 设置默认值
	pageVal := 1
	if page != nil {
		pageVal = *page
	}

	limitVal := 10
	if limit != nil {
		limitVal = *limit
	}

	sortByVal := "ownership"
	if sortBy != nil {
		sortByVal = *sortBy
	}

	// 获取代币持有者
	holders, err := db.GetTokenHolders(tokenIDInt, pageVal, limitVal, sortByVal)
	if err != nil {
		log.Printf("获取代币持有者失败: %v", err)
		return nil, err
	}

	// 创建解析器
	resolvers := make([]*TokenHolderResolver, len(holders))
	for i, holder := range holders {
		resolvers[i] = &TokenHolderResolver{holder: holder}
	}

	// 转换为GraphQL模型
	result := make([]*models.TokenHolder, len(resolvers))
	for i, resolver := range resolvers {
		result[i] = &models.TokenHolder{
			AccountID:    resolver.AccountId(),
			TokenID:      resolver.TokenId(),
			TokenAddress: resolver.TokenAddress(),
			Ownership:    resolver.Ownership(),
			ValueUsd:     resolver.ValueUSD(),
		}
	}

	return result, nil
}

func formatTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format("2006-01-02 15:04:05")
}

func formatNullTime(t sql.NullTime) string {
	if !t.Valid {
		return ""
	}
	return t.Time.Format("2006-01-02 15:04:05")
}

// createTokenRecentMetric 创建TokenRecentMetric对象
func createTokenRecentMetric(tokenID int, timeWindow string) *models.TokenRecentMetric {
	// 获取基本指标
	recentMetric, err := db.GetTokenRecentMetricByTokenID(tokenID, timeWindow, "all")
	if err != nil {
		log.Printf("获取代币最近指标失败: %v", err)
		return createDefaultTokenRecentMetric()
	}

	if recentMetric == nil {
		return createDefaultTokenRecentMetric()
	}

	// 获取fresh wallet指标
	freshWalletMetric, err := db.GetTokenRecentMetricByTokenID(tokenID, timeWindow, "fresh_wallet")
	if err != nil {
		log.Printf("获取fresh wallet指标失败: %v", err)
	}

	// 获取smart money指标
	smartMoneyMetric, err := db.GetTokenRecentMetricByTokenID(tokenID, timeWindow, "smart_money")
	if err != nil {
		log.Printf("获取smart money指标失败: %v", err)
	}

	// 获取exchange指标
	exchangeMetric, err := db.GetTokenRecentMetricByTokenID(tokenID, timeWindow, "cex")
	if err != nil {
		log.Printf("获取exchange指标失败: %v", err)
	}

	// 设置inflow和outflow值
	var freshWalletInflow float64
	if freshWalletMetric != nil {
		freshWalletInflow = freshWalletMetric.BuyVolumeUSD
	}

	var smartMoneyInflow, smartMoneyOutflow float64
	if smartMoneyMetric != nil {
		smartMoneyInflow = smartMoneyMetric.BuyVolumeUSD
		smartMoneyOutflow = smartMoneyMetric.SellVolumeUSD
	}

	var exchangeInflow, exchangeOutflow float64
	if exchangeMetric != nil {
		exchangeInflow = exchangeMetric.BuyVolumeUSD
		exchangeOutflow = exchangeMetric.SellVolumeUSD
	}

	// 设置TimeWindow枚举值
	var timeWindowEnum models.TimeWindow
	switch timeWindow {
	case "20s":
		timeWindowEnum = models.TimeWindowTwentySeconds
	case "1min":
		timeWindowEnum = models.TimeWindowOneMinute
	case "5min":
		timeWindowEnum = models.TimeWindowFiveMinutes
	case "1h":
		timeWindowEnum = models.TimeWindowOneHour
	default:
		timeWindowEnum = models.TimeWindowOneMinute
	}

	return &models.TokenRecentMetric{
		TimeWindow:        timeWindowEnum,
		Txcnt:             recentMetric.TxCnt,
		Volume:            recentMetric.VolumeUSD,
		PriceChange:       0, // 简化处理，实际需要计算
		Buys:              recentMetric.BuyCount,
		Sells:             recentMetric.SellCount,
		BuyVolume:         recentMetric.BuyVolumeUSD,
		SellVolume:        recentMetric.SellVolumeUSD,
		FreshWalletInflow: freshWalletInflow,
		SmartMoneyInflow:  smartMoneyInflow,
		SmartMoneyOutflow: smartMoneyOutflow,
		ExchangeInflow:    exchangeInflow,
		ExchangeOutflow:   exchangeOutflow,
		BuyPressure:       recentMetric.BuyPressureUSD,
	}
}

// createDefaultTokenRecentMetric 创建默认的TokenRecentMetric对象
func createDefaultTokenRecentMetric() *models.TokenRecentMetric {
	return &models.TokenRecentMetric{
		TimeWindow:        models.TimeWindowOneMinute,
		Txcnt:             0,
		Volume:            0,
		PriceChange:       0,
		Buys:              0,
		Sells:             0,
		BuyVolume:         0,
		SellVolume:        0,
		FreshWalletInflow: 0,
		SmartMoneyInflow:  0,
		SmartMoneyOutflow: 0,
		ExchangeInflow:    0,
		ExchangeOutflow:   0,
		BuyPressure:       0,
	}
}
