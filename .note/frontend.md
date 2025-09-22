# 
## account

通过选项卡选择account

### account详情

1. account信息
    1. entity,label(聪明钱，巨鲸，交易所，fresh wallet)
    2. balance信息
        1. eth
        2. erc20 token:balance,price,value_usd
        3. defi position:protocol,contract_address,position,value_usd
2. transfer列表 选择buy或sell,每页10个
    1. **字段transaction_hash,block_timestamp,from,to,token,token_address,value,value_usd**
3. accountTransferHistory，每页十个
    1. transfer按5分钟聚合指标，与整点对齐
    2. 指标：transfer数，总value_usd

## token

### token列表页

一页展示10个token,按mcap排序

选项卡token info和token flow

选择token info时展示chainName,tokenSymbol,price,priceChange5min,Mcap,liquidity,fdv,buyPressure5min

选择token flow时展示 5分钟内 fresh wallet inflow,smart money inflow\outflow,exchange inflow\outflow

### token详情页

1. 大盘：展示token info全部
2. token信息：发行商，token age,token 类型，security_score
3. 实时指标展示
    1. token最近20s\1min\5min\1h指标 使用tokenRecentMetric
    2. 通过选项卡时间范围
    3.  指标包括：交易数，volume,priceChange，buys,sellers buy vol,sell vol
    4. 其中buys,sellers buy vol,sell vol通过一条定长的红绿对比线段显示
4. 柱状图\折线图
    1. 选项卡：价格，mcap
    2. 时间间隔均为20s，与零点对齐，使用tokenRollingMetric
5. transfer
    1. 筛选条件：买\卖 
    2. 通过列表展示，from,to,value_usd,block_timestamp,每页10个
6. topHolder
    1. 列表展示，每页10个，按ownership排序
    2. ownership（占有百分比），value_usd
   
```graphql
#######################################
# Account 维度
#######################################

# 账户详细信息
type Account {
  id: ID!
  # account所属实体
  entity: String
  # 标签列表，可选值：聪明钱、巨鲸、交易所、fresh wallet
  labels: [String!]!
  chainName:String!
  # 余额信息
  ethBalance: Float!
  erc20Balances: [ERC20Balance!]!
  defiPositions: [DefiPosition!]!
  
  # 交易记录列表
  accountTransferEvents(page: Int, limit: Int): [AccountTransferEvent!]!
  
  # 交易历史，按照5分钟聚合且与整点对齐
  accountTransferHistory(page: Int, limit: Int): [AccountTransferHistory!]!
}

# ERC20 余额
type ERC20Balance {
  tokenAddress: String!
  tokenSymbol:String!
  balance: Float!
  price: Float!
  valueUSD: Float!
}

# DeFi 持仓信息
type DefiPosition {
  protocol: String!
  contractAddress: String!
  position: String!
  valueUSD: Float!
}

# 单笔交易
type AccountTransferEvent {
  transactionHash: String!
  blockTimestamp: String!
  from_address: String!
  buyOrSell:String!
  to_address: String!
  tokenSymbol: String!
  tokenAddress: String!
  value : Float!
  valueUSD: Float!
}

# transfer历史聚合数据（5分钟为单位）
type AccountTransferHistory {
  # 对应5分钟时间段的结束时间（与整点对齐）
  endTime: String!
  txCnt: Int!
  totalValueUSD: Float!
}

#######################################
# Token 维度
#######################################

# 用于列表页展示的 token 基本信息（包括 token info 与 token flow 两种视图）
type Token {
  id: ID!
  # token info 字段
  chainName: String!
  tokenSymbol: String!

  tokenDetail:[TokenDetail!]!
}

# token 详情页：展示更全面的信息
type TokenDetail {
  id: ID!
  # 大盘信息（与 Token.info 部分相同）
  chainName: String!
  tokenSymbol: String!
  price: Float!
  mcap: Float!
  liquidity: Float!
  fdv: Float!
  
  # token 额外信息
  issuer: String!
  tokenAge: String!
  tokenCatagory: String!
  securityScore: Float!
  
  # 实时指标（基于不同时间窗口，前端可通过 tab 切换）
  recentMetrics(timeWindow: timeWindow!): TokenRecentMetric!
  
  # 历史走势数据（柱状图或折线图，时间间隔均为20s，与整点对齐）
  rollingMetrics: [TokenRollingMetric!]!
  
  # 交易列表（支持筛选：买/卖、onlySmartMoney、onlyWithCex、onlyWithDex，每页10条）
  tokenTransferEvents( page: Int, limit: Int): [tokenTransferEvent!]!
  
  # Top Holder 列表（按 ownership 排序，每页10条）
  tokenHolders(page: Int, limit: Int,sortBy: ownership): [TokenHolder!]!
}

# 实时指标（tokenRecentMetric）
enum timeWindow {
  TWENTY_SECONDS
  ONE_MINUTE
  FIVE_MINUTES
  ONE_HOUR
}

type TokenRecentMetric {
  timeWindow: timeWindow!
  txcnt: Int!
  volume: Float!
  priceChange: Float!
  buys: Int!
  sells: Int!
  buyVolume: Float!
  sellVolume: Float!
  
  freshWalletInflow: Float!
  smartMoneyInflow: Float!
  smartMoneyOutflow: Float!
  exchangeInflow: Float!
  exchangeOutflow: Float!
  buyPressure: Float!
}

# 历史走势数据（tokenRollingMetric）
type TokenRollingMetric {
  endTime: String!  # 每20秒的时间点（与整点对齐）
  price: Float!
  mcap: Float!
}

# token 交易记录（列表页与详情页使用，字段较少）
type TokenTransferEvent {
  transactionHash: String!
  fromAddress: String!
  toAddress: String!
  valueUSD: Float!
  blockTimestamp: String!
}


# Top Holder 信息
type TokenHolder {
  address: String!
  ownership: Float!  # 占有百分比
  valueSD: Float!
}

#######################################
# Query 根接口
#######################################

# 查询示例
query {
  account(id: "account123") {
    entity
    labels
    ethBalance
    erc20Balances {
      tokenAddress
      balance
      price
      valueUSD
    }
    defiPositions {
      protocol
      contractAddress
      position
      valueUSD
    }
    accountTransferEvents(page: 1, limit: 10, buyOrSell:"buy" ,sortBy: blockTimestamp) {
      transactionHash
      blockTimestamp
      from_address
      to_address
      tokenSymbol
      tokenAddress
      value
      valueUSD
    }
    accountTransferHistory(page: 1, limit: 10, sortBy: endTime) {
      endTime
      txcnt
      totalValueUSD
    }
  }
}
query {
  accountTransferEvents(
    accountId: "account123"
    page: 1
    limit: 10
    buyOrSell:"buy"
    sortBy: blockTimestamp
  ) {
    transactionHash
    blockTimestamp
    from_address
    to_address
     tokenSymbol
      tokenAddress
      valueUSD
  }
}
query {
  accountTransferHistory(
    accountId: "account123"
    page: 1
    limit: 10
    sortBy: endTime
  ) {
    transactionHash
    blockTimestamp
    from_address
    to_address
     tokenSymbol
      tokenAddress
      value
      valueUSD
  }
}
#查询token列表
query {
  token(page: 1, limit: 10, sortBy: mcap) {
    tokenSymbol
    chainName
    tokenDetail(){
    mcap
    fdv
    liquidity
    price
    tokenRecentMetric(){
    priceChange5min
	  buyPressure5min
    }
	  }
  }
}
##查询token详情
query {
  token(tokenId: "tokenABC") {
	  #元数据
    tokenSymbol
    chainName
    tokenDetail{
     issuer
    tokenAge
    tokenCatagory
    price
    mcap
    liquidity
    fdv
    securityScore
    recentMetrics(timeWindow: ONE_MINUTE) {
      timeWindow
      transactionCount
      volume
      priceChange
      buys
      sellers
      buyVolume
      sellVolume
      priceChange
      freshWalletInflow
		  smartMoneyInflow
		  smartMoneyOutflow
		  exchangeInflow
		  exchangeOutflow
    }
    rollingMetrics {
      endTime
      price
      mcap
    }
    tokenTransferEvent(page: 1, limit: 10,sortBy: blockTimestamp) {
      transactionHash
      from_address
      to_address
      valueUSD
      blockTimestamp
    }
    tokenHolder(page: 1, limit: 10,sortBy: ownership) {
      address
      ownership
      valueUSD
    }
    }
  }
```