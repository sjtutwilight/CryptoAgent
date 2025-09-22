package resolvers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/yangguang/Twilight/UniQuery/db"
	"github.com/yangguang/Twilight/UniQuery/models"
)

// Account 解析器
type AccountResolver struct {
	account *models.AccountDB
}

// NewAccountResolver 创建账户解析器
func NewAccountResolver(account *models.AccountDB) *AccountResolver {
	return &AccountResolver{account: account}
}

// ID 解析账户ID
func (r *AccountResolver) ID() string {
	return strconv.Itoa(r.account.ID)
}

// Entity 解析账户实体
func (r *AccountResolver) Entity() *string {
	return &r.account.Entity
}

// Labels 解析账户标签
func (r *AccountResolver) Labels() []string {
	labels := []string{}

	if r.account.SmartMoneyTag {
		labels = append(labels, "聪明钱")
	}
	if r.account.CexTag {
		labels = append(labels, "交易所")
	}
	if r.account.BigWhaleTag {
		labels = append(labels, "巨鲸")
	}
	if r.account.FreshWalletTag {
		labels = append(labels, "fresh account")
	}

	return labels
}

// ChainName 解析链名称
func (r *AccountResolver) ChainName() string {
	return r.account.ChainName
}

// Address 解析账户地址
func (r *AccountResolver) Address() string {
	return r.account.Address
}

// EthBalance 解析ETH余额
func (r *AccountResolver) EthBalance() float64 {
	// 获取native资产
	assets, err := db.GetAccountAssetsByAccountID(r.account.ID, "native")
	if err != nil {
		log.Printf("获取ETH余额失败: %v", err)
		return 0
	}

	// 如果没有native资产，返回0
	if len(assets) == 0 {
		return 0
	}

	// 返回第一个native资产的value_usd
	return assets[0].ValueUSD
}

// ERC20Balances 解析ERC20余额
func (r *AccountResolver) ERC20Balances() []*ERC20BalanceResolver {
	// 获取erc20资产
	assets, err := db.GetAccountAssetsByAccountID(r.account.ID, "erc20")
	if err != nil {
		log.Printf("获取ERC20余额失败: %v", err)
		return []*ERC20BalanceResolver{}
	}

	// 转换为解析器
	resolvers := make([]*ERC20BalanceResolver, len(assets))
	for i, asset := range assets {
		resolvers[i] = &ERC20BalanceResolver{asset: asset}
	}

	return resolvers
}

// DefiPositions 解析DeFi持仓
func (r *AccountResolver) DefiPositions() []*DefiPositionResolver {
	// 获取defiPosition资产
	assets, err := db.GetAccountAssetsByAccountID(r.account.ID, "defiPosition")
	if err != nil {
		log.Printf("获取DeFi持仓失败: %v", err)
		return []*DefiPositionResolver{}
	}

	// 转换为解析器
	resolvers := make([]*DefiPositionResolver, len(assets))
	for i, asset := range assets {
		resolvers[i] = &DefiPositionResolver{asset: asset}
	}

	return resolvers
}

// AccountTransferEvents 获取账户的转账事件
func (r *AccountResolver) AccountTransferEvents(ctx context.Context, page *int, limit *int, buyOrSell *string, sortBy *string) []*AccountTransferEventResolver {
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

	sortByVal := ""
	if sortBy != nil {
		sortByVal = *sortBy
	}

	// 记录查询参数
	log.Printf("查询账户转账事件，账户ID: %d, 页码: %d, 每页数量: %d, 买卖: %s, 排序: %s",
		r.account.ID, pageVal, limitVal, buyOrSellVal, sortByVal)

	// 获取转账事件
	events, err := db.GetAccountTransferEvents(r.account.ID, pageVal, limitVal, buyOrSellVal, sortByVal)
	if err != nil {
		log.Printf("获取账户转账事件失败: %v", err)
		return []*AccountTransferEventResolver{}
	}

	// 记录返回的事件数量
	log.Printf("返回账户转账事件数量: %d", len(events))

	// 转换为解析器
	resolvers := make([]*AccountTransferEventResolver, len(events))
	for i, event := range events {
		// 检查event是否为nil
		if event == nil {
			log.Printf("警告: 第%d个事件为nil", i)
			continue
		}

		// 创建解析器
		resolvers[i] = &AccountTransferEventResolver{event: event, accountId: r.account.ID}

		// 记录事件信息
		log.Printf("事件%d: ID=%d, 发送方=%s, 接收方=%s, 价值=%f",
			i, event.ID, event.FromAddress, event.ToAddress, event.ValueUSD)
	}

	return resolvers
}

// AccountTransferHistory 解析账户转账历史
func (r *AccountResolver) AccountTransferHistory(ctx context.Context, page *int, limit *int, buyOrSell *string, sortBy *string) []*AccountTransferHistoryResolver {
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
	history, err := db.GetAccountTransferHistory(r.account.ID, p, l, b, s)
	if err != nil {
		log.Printf("获取账户转账历史失败: %v", err)
		return []*AccountTransferHistoryResolver{}
	}

	// 转换为解析器
	resolvers := make([]*AccountTransferHistoryResolver, len(history))
	for i, h := range history {
		resolvers[i] = &AccountTransferHistoryResolver{history: h}
	}

	return resolvers
}

// ERC20Balance 解析器
type ERC20BalanceResolver struct {
	asset *models.AccountAssetViewDB
}

// TokenAddress 解析代币地址
func (r *ERC20BalanceResolver) TokenAddress() string {
	// 这里需要根据biz_id获取token_address
	token, err := db.GetTokenByID(r.asset.BizID)
	if err != nil {
		log.Printf("获取代币地址失败: %v", err)
		return ""
	}
	return token.TokenAddress
}

// TokenSymbol 解析代币符号
func (r *ERC20BalanceResolver) TokenSymbol() string {
	return r.asset.BizName
}

// Balance 解析余额
func (r *ERC20BalanceResolver) Balance() float64 {
	return r.asset.Value
}

// Price 解析价格
func (r *ERC20BalanceResolver) Price() float64 {
	return r.asset.AssetPrice
}

// ValueUSD 解析美元价值
func (r *ERC20BalanceResolver) ValueUSD() float64 {
	return r.asset.ValueUSD
}

// DefiPosition 解析器
type DefiPositionResolver struct {
	asset *models.AccountAssetViewDB
}

// Protocol 解析协议
func (r *DefiPositionResolver) Protocol() string {
	// 根据BizName判断协议
	// 如果BizName包含"-"，可能是交易对，返回TWSwap
	if strings.Contains(r.asset.BizName, "-") {
		return "TWSwap"
	}
	// 否则返回一个通用的协议名称
	return "DeFi"
}

// ContractAddress 解析合约地址
func (r *DefiPositionResolver) ContractAddress() string {
	// 记录BizID和BizName
	log.Printf("获取交易对地址，BizID: %d, BizName: %s", r.asset.BizID, r.asset.BizName)

	// 如果BizName包含"-"，可能是交易对，尝试从数据库获取地址
	if strings.Contains(r.asset.BizName, "-") {
		// 获取pair地址
		pairAddress, err := db.GetPairAddressByID(r.asset.BizID)
		if err == nil && pairAddress != "" {
			log.Printf("获取到交易对地址: %s", pairAddress)
			return pairAddress
		}

		// 如果获取失败，尝试从BizName构造一个地址
		// 例如，如果BizName是"USDC-WETH"，则构造地址为"0xUSDC-WETH-{BizID}"
		log.Printf("无法从数据库获取交易对地址，使用BizName构造地址: %s", r.asset.BizName)
		return fmt.Sprintf("0x%s-%d", r.asset.BizName, r.asset.BizID)
	}

	// 如果不是交易对，返回一个基于BizID的地址
	log.Printf("非交易对，使用BizID生成地址: %d", r.asset.BizID)
	return fmt.Sprintf("0x%x", r.asset.BizID)
}

// Position 解析持仓
func (r *DefiPositionResolver) Position() string {
	return r.asset.BizName
}

// ValueUSD 解析美元价值
func (r *DefiPositionResolver) ValueUSD() float64 {
	return r.asset.ValueUSD
}

// AccountTransferEvent 解析器
type AccountTransferEventResolver struct {
	event     *models.TransferEventDB
	accountId int
}

// AccountId 解析账户ID
func (r *AccountTransferEventResolver) AccountId() string {
	return strconv.Itoa(r.accountId)
}

// BlockTimestamp 解析区块时间戳
func (r *AccountTransferEventResolver) BlockTimestamp() string {
	// 检查event是否为nil
	if r.event == nil {
		log.Printf("警告: 事件为nil")
		return ""
	}

	// 检查BlockTimestamp字段是否有效
	if r.event.BlockTimestamp.Valid {
		return r.event.BlockTimestamp.Time.Format("2006-01-02T15:04:05Z")
	}

	// 如果BlockTimestamp无效，尝试使用Timestamp
	if r.event.Timestamp.Valid {
		return r.event.Timestamp.Time.Format("2006-01-02T15:04:05Z")
	}

	// 如果没有有效的时间戳，返回当前时间
	log.Printf("警告: 事件ID=%d 没有有效的时间戳", r.event.ID)
	return time.Now().Format("2006-01-02T15:04:05Z")
}

// FromAddress 解析发送地址
func (r *AccountTransferEventResolver) FromAddress() string {
	return r.event.FromAddress
}

// BuyOrSell 解析买卖方向
func (r *AccountTransferEventResolver) BuyOrSell() string {
	// 获取账户地址
	var accountAddress string
	err := db.DB.Get(&accountAddress, "SELECT address FROM account WHERE id = $1", r.accountId)
	if err != nil {
		log.Printf("获取账户地址失败: %v", err)
		return "unknown"
	}

	// 将地址转换为小写进行比较
	lowerAccountAddress := strings.ToLower(accountAddress)
	lowerToAddress := strings.ToLower(r.event.ToAddress)
	lowerFromAddress := strings.ToLower(r.event.FromAddress)

	// 如果账户是接收方，则为买入
	if lowerToAddress == lowerAccountAddress {
		return "buy"
	}
	// 如果账户是发送方，则为卖出
	if lowerFromAddress == lowerAccountAddress {
		return "sell"
	}

	// 默认情况
	return "unknown"
}

// ToAddress 解析接收地址
func (r *AccountTransferEventResolver) ToAddress() string {
	return r.event.ToAddress
}

// TokenSymbol 解析代币符号
func (r *AccountTransferEventResolver) TokenSymbol() string {
	return r.event.TokenSymbol
}

// TokenAddress 解析代币地址
func (r *AccountTransferEventResolver) TokenAddress() string {
	// 这里需要根据token_id获取token_address
	token, err := db.GetTokenByID(r.event.TokenID)
	if err != nil {
		log.Printf("获取代币地址失败: %v", err)
		return ""
	}
	return token.TokenAddress
}

// Value 解析值
func (r *AccountTransferEventResolver) Value() float64 {
	// 这里需要从event中获取value
	// 简化处理，返回0
	return 0
}

// ValueUSD 解析美元价值
func (r *AccountTransferEventResolver) ValueUSD() float64 {
	return r.event.ValueUSD
}

// AccountTransferHistory 解析器
type AccountTransferHistoryResolver struct {
	history *models.AccountTransferHistoryDB
}

// EndTime 解析结束时间
func (r *AccountTransferHistoryResolver) EndTime() string {
	return r.history.EndTime.Format("2006-01-02T15:04:05Z")
}

// BuyOrSell 解析买卖类型
func (r *AccountTransferHistoryResolver) BuyOrSell() string {
	if r.history.IsBuy == 1 {
		return "buy"
	}
	return "sell"
}

// TxCnt 解析交易数量
func (r *AccountTransferHistoryResolver) TxCnt() int {
	return r.history.TxCnt
}

// TotalValueUSD 解析总美元价值
func (r *AccountTransferHistoryResolver) TotalValueUSD() float64 {
	return r.history.TotalValueUSD
}
