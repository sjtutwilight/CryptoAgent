
# **查询器 (UniQuery)**

UniQuery 是为前端提供查询服务的模块，以可视化方式呈现交易趋势、代币信息、账户信息等数据。

## **GraphQL Schema**

### **查询根类型**

```graphql
type Query {
  # Account 相关查询
  account(id: ID!): Account
  accounts(page: Int = 1, limit: Int = 10): [Account!]!
  accountTransferEvents(
    accountId: ID!,
    page: Int = 1,
    limit: Int = 10,
    buyOrSell: String,
    sortBy: String = "blockTimestamp"
  ): [AccountTransferEvent!]!
  accountTransferHistory(
    accountId: ID!,
    page: Int = 1,
    limit: Int = 10,
    buyOrSell: String,
    sortBy: String = "endTime"
  ): [AccountTransferHistory!]!

  # Token 相关查询
  token(id: ID!): Token
  tokens(
    page: Int = 1,
    limit: Int = 10,
    sortBy: String = "mcap"
  ): [Token!]!
  tokenTransferEvents(
    tokenId: ID!,
    page: Int = 1,
    limit: Int = 10,
    buyOrSell: String,
    onlySmartMoney: Boolean = false,
    onlyWithCex: Boolean = false,
    onlyWithDex: Boolean = false,
    sortBy: String = "blockTimestamp"
  ): [TokenTransferEvent!]!
  tokenHolders(
    tokenId: ID!,
    page: Int = 1,
    limit: Int = 10,
    sortBy: String = "ownership"
  ): [TokenHolder!]!
}

```

### **Account 相关类型**

```graphql
# 账户详细信息
type Account {
  id: ID!
  entity: String                # 账户所属实体
  labels: [String!]!            # 标签列表，可选值：聪明钱、巨鲸、交易所、fresh account
  chainName: String!
  address: String!

  # 余额信息
  ethBalance: Float!
  erc20Balances: [ERC20Balance!]!
  defiPositions: [DefiPosition!]!

  # 交易记录列表
  accountTransferEvents(
    page: Int = 1,
    limit: Int = 10,
    buyOrSell: String,
    sortBy: String = "blockTimestamp"
  ): [AccountTransferEvent!]!

  # 交易历史，按照5分钟聚合且与整点对齐
  accountTransferHistory(
    page: Int = 1,
    limit: Int = 10,
    buyOrSell: String,
    sortBy: String = "endTime"
  ): [AccountTransferHistory!]!
}

# ERC20 余额
type ERC20Balance {
  tokenAddress: String!
  tokenSymbol: String!
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
  accountId: ID!
  blockTimestamp: String!
  fromAddress: String!
  buyOrSell: String!
  toAddress: String!
  tokenSymbol: String!
  valueUSD: Float!
}

# 转账历史聚合数据（5分钟为单位）
type AccountTransferHistory {
  endTime: String!            # 对应5分钟时间段的结束时间（与整点对齐）
  buyOrSell: String!
  txCnt: Int!
  totalValueUSD: Float!
}

```

### **Token 相关类型**

```graphql
# 代币信息
type Token {
  id: ID!
  chainName: String!
  tokenSymbol: String!
  tokenAddress: String!

  # 详细信息
  tokenDetail: TokenDetail
}

# 代币详细信息
type TokenDetail {
  id: ID!
  # 大盘信息
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
  recentMetrics(timeWindow: TimeWindow!): TokenRecentMetric!

  # 历史走势数据（柱状图或折线图，时间间隔均为20s，与整点对齐）
  rollingMetrics(
    limit: Int = 100,
    startTime: String,
    endTime: String
  ): [TokenRollingMetric!]!

  # 交易列表
  tokenTransferEvents(
    page: Int = 1,
    limit: Int = 10,
    buyOrSell: String,
    onlySmartMoney: Boolean = false,
    onlyWithCex: Boolean = false,
    onlyWithDex: Boolean = false,
    sortBy: String = "blockTimestamp"
  ): [TokenTransferEvent!]!

  # Top Holder 列表
  tokenHolders(
    page: Int = 1,
    limit: Int = 10,
    sortBy: String = "ownership"
  ): [TokenHolder!]!
}

# 时间窗口枚举
enum TimeWindow {
  TWENTY_SECONDS
  ONE_MINUTE
  FIVE_MINUTES
  ONE_HOUR
}

# 代币实时指标
type TokenRecentMetric {
  timeWindow: TimeWindow!
  txcnt: Int!
  volume: Float!
  priceChange: Float!          # 窗口前后价格变化
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

# 代币滚动指标
type TokenRollingMetric {
  endTime: String!
  price: Float!
  mcap: Float!
}

# 代币转账事件
type TokenTransferEvent {
  tokenId: ID!
  fromAddress: String!
  toAddress: String!
  valueUSD: Float!
  blockTimestamp: String!
}

# 代币持有者
type TokenHolder {
  accountId: ID!
  tokenId: ID!
  tokenAddress: String!
  ownership: Float!
  valueUSD: Float!
}

```

## **查询示例**

### **查询账户信息**

```graphql
query GetAccount($id: ID!) {
  account(id: $id) {
    id
    entity
    labels
    chainName
    address
    ethBalance
    erc20Balances {
      tokenAddress
      tokenSymbol
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
  }
}

```

### **查询账户转账事件**

```graphql
query GetAccountTransferEvents($accountId: ID!, $page: Int, $limit: Int, $buyOrSell: String, $sortBy: String) {
  accountTransferEvents(
    accountId: $accountId
    page: $page
    limit: $limit
    buyOrSell: $buyOrSell
    sortBy: $sortBy
  ) {
    accountId
    blockTimestamp
    fromAddress
    toAddress
    tokenSymbol
    valueUSD
    buyOrSell
  }
}

```

### **查询账户转账历史**

```graphql
query GetAccountTransferHistory($accountId: ID!, $page: Int, $limit: Int) {
  account(id: $accountId) {
    accountTransferHistory(page: $page, limit: $limit) {
      endTime
      buyOrSell
      txCnt
      totalValueUSD
    }
  }
}

```

### **查询代币信息**

```graphql
query GetToken($id: ID!) {
  token(id: $id) {
    id
    chainName
    tokenSymbol
    tokenAddress
    tokenDetail {
      price
      mcap
      liquidity
      fdv
      issuer
      tokenAge
      tokenCatagory
      securityScore
      recentMetrics(timeWindow: ONE_MINUTE) {
        timeWindow
        txcnt
        volume
        priceChange
        buys
        sells
        buyVolume
        sellVolume
        freshWalletInflow
        smartMoneyInflow
        smartMoneyOutflow
        exchangeInflow
        exchangeOutflow
        buyPressure
      }
    }
  }
}

```

### **查询代币转账事件**

```graphql
query GetTokenTransferEvents($tokenId: ID!, $page: Int, $limit: Int, $buyOrSell: String, $onlySmartMoney: Boolean, $onlyWithCex: Boolean, $onlyWithDex: Boolean, $sortBy: String) {
  tokenTransferEvents(
    tokenId: $tokenId
    page: $page
    limit: $limit
    buyOrSell: $buyOrSell
    onlySmartMoney: $onlySmartMoney
    onlyWithCex: $onlyWithCex
    onlyWithDex: $onlyWithDex
    sortBy: $sortBy
  ) {
    tokenId
    fromAddress
    toAddress
    valueUSD
    blockTimestamp
  }
}

```

### **查询代币持有者**

```graphql
query GetTokenHolders($tokenId: ID!, $page: Int, $limit: Int, $sortBy: String) {
  token(id: $tokenId) {
    tokenDetail {
      tokenHolders(page: $page, limit: $limit, sortBy: $sortBy) {
        accountId
        tokenId
        tokenAddress
        ownership
        valueUSD
      }
    }
  }
}

```

### **查询代币滚动指标**

```graphql
query GetTokenRollingMetrics($tokenId: ID!, $limit: Int) {
  token(id: $tokenId) {
    tokenDetail {
      rollingMetrics(limit: $limit) {
        endTime
        price
        mcap
      }
    }
  }
}

```
