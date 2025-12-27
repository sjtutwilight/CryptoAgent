# LocalNode 设计文档

## 模块定位
本地区块链节点环境，基于Hardhat搭建的DEX合约与交易仿真器。

## 核心功能
- 自建DEX：参考Uniswap V2，基于Solidity 0.8.x
- 交易仿真器：模拟mint、添加流动性、swap等操作
- 账户标签：CEX、聪明钱、Public Figure、Fresh Wallet

## 技术栈
- Hardhat
- Solidity 0.8.x
- Ethers.js

## 数据流
```
部署脚本 → 合约部署 → 仿真器 → 链上交易
                                ↓
                            Worker监听
```

## 关键设计
- 完整DEX实现（Router、Factory、Pair）
- 可配置交易场景（流动性注入、交易频率）
- 支持账户标签体系

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架










