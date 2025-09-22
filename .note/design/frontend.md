## 代币大盘

### 元信息

1. chainName
2. name/symbol
3. age（随机值）
4. token类型
5. 介绍信息
6. security_score（随机值）

### 代币宏观指标

1. 价格相关
    1. 当前price
    2. 历史价格（折线图，价格涨跌幅）
2. 代币状态分布
    1. fdv
    2. mcap，市值变化
    3. liquidity
    4. 三者比值

## 交易流

1. DEX 交易量
2. 分买卖：交易量，交易数，近期top买卖地址信息
3. **netflow：分标签：**Public Figure，Whale，Top PnL，Exchange，Fresh Wallets，聪明钱

## pnl

1. top pnl信息
    1.  **Total PnL， Total ROI%，Realized PnL，Unrealized PnL，% Still Holding**
2. NUPL ，SOPR，MVRV

## 代币分布

- 宏观指标
    - top 2持仓占比、Median Holder，Fresh Wallets holder占比、holder数
    - 代币集中度（通过宏观代币指标简单量化得到）
- 标签维度指标（Public Figure，Whale，Exchange，Fresh Wallets，聪明钱）
    - 持仓balance
    - 持仓balance 1min变化百分比
    - 持仓数折线图，横坐标时间（粒度1min），纵坐标持仓balance
- top holder明细
    - account地址、标签、ownership百分比、balance、value_usd.

## account

1. account信息
    1. entity
    2. label
    3. 地址
2. 资产
    1. 资产类型
        1. native资产，如eth
        2. erc20
        3. defi position
    2. dex transfer列表
        1. 字段transaction_hash,block_timestamp,from,to,token,token_address,value,value_usd**
    3.  accountTransferHistory
        1. transfer按5分钟聚合指标，与整点对齐
        2. 指标：transfer数，总value_usd