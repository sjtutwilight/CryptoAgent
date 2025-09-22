# UniQuery - DeFi数据服务平台查询器

UniQuery 是 DeFi 数据服务平台的查询服务组件，提供 GraphQL API 接口，用于查询链上数据、账户信息、代币信息等。

## 项目结构

```
UniQuery/
├── db/                  # 数据库访问层
│   ├── db.go            # 数据库连接
│   ├── account_repository.go  # 账户相关数据库操作
│   └── token_repository.go    # 代币相关数据库操作
├── graph/               # GraphQL 相关
│   ├── schema.graphql   # GraphQL 模式定义
│   └── generated/       # 生成的 GraphQL 代码
├── models/              # 数据模型
│   ├── account.go       # 账户相关模型
│   └── token.go         # 代币相关模型
├── resolvers/           # GraphQL 解析器
│   ├── account_resolver.go    # 账户相关解析器
│   ├── token_resolver.go      # 代币相关解析器
│   ├── query_resolver.go      # 查询解析器
│   └── enums.go               # 枚举类型定义
├── server/              # 服务器
│   └── server.go        # 服务器入口
├── main.go              # 主程序入口
├── go.mod               # Go 模块定义
└── README.md            # 项目说明
```

## 功能特性

- 账户查询：查询账户信息、资产、转账历史等
- 代币查询：查询代币信息、指标、持有者等
- 实时指标：查询代币实时指标、滚动指标等
- 转账事件：查询账户和代币的转账事件

## 数据库映射

UniQuery 将数据库表映射到 GraphQL 类型，主要映射关系如下：

### 账户相关

- `account` 表 → `Account` 类型
- `account_asset_view` 表 → `ERC20Balance` 和 `DefiPosition` 类型
- `account_transfer_history` 表 → `AccountTransferHistory` 类型
- `transfer_event` 表 → `AccountTransferEvent` 类型

### 代币相关

- `token` 表 + `token_metric` 表 → `Token` 和 `TokenDetail` 类型
- `token_recent_metric` 表 → `TokenRecentMetric` 类型
- `token_rolling_metric` 表 → `TokenRollingMetric` 类型
- `token_holder` 表 → `TokenHolder` 类型

## 安装和运行

### 前置条件

- Go 1.16+
- PostgreSQL 数据库

### 环境变量

创建 `.env` 文件，设置以下环境变量：

```
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=defi_data
DB_SSLMODE=disable
PORT=8080
```

### 运行

```bash
# 安装依赖
go mod tidy

# 生成 GraphQL 代码
go run github.com/99designs/gqlgen generate

# 运行服务
go run main.go
```

访问 http://localhost:8080 查看 GraphQL playground。

## 查询示例

### 查询账户信息

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

### 查询代币信息

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

## 注意事项

- 本项目需要配合 DeFi 数据服务平台的其他组件一起使用，如交易模拟器、链监听器、链处理器等。
- 数据库表结构需要与项目约定一致，详见项目根目录的 README.md。 

## 映射逻辑
AccountTransferEvent:
accountId对应地址为p
 1.buyOrSell为”buy” select * from transfer_event where from_address =p 
 2. buyOrSell为”sell” select * from transfer_event where to_address =p  
  
TokenTransferEvent: select * from transfer_event where token_id ={tokenId}