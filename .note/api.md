# Mock数据源API文档
## WebSocket API
### 连接端点
```
ws://localhost:8090/ws
```
### 1. eth_subscribe - 订阅新区块头
**描述**: 订阅以太坊新区块头推送，每12秒推送一次新区块。
**请求格式**:
```json
{
  "id": 1,
  "method": "eth_subscribe",
  "params": ["newHeads"],
  "jsonrpc": "2.0"
}
```
**响应格式**:
```json
{
  "id": 1,
  "result": "0x9ce59a13059e417087c02d3236a0b1cc",
  "jsonrpc": "2.0"
}
```
**区块头推送格式**:
```json
{
  "jsonrpc": "2.0",
  "method": "eth_subscription",
  "params": {
    "subscription": "0x9ce59a13059e417087c02d3236a0b1cc",
    "result": {
      "number": "0xf4242",
      "hash": "0x1234567890abcdef1234567890abcdef12345678",
      "parentHash": "0x0987654321fedcba0987654321fedcba09876543",
      "nonce": "0x0000000000000000",
      "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
      "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
      "stateRoot": "0xabcdef1234567890abcdef1234567890abcdef12",
      "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
      "miner": "0x0000000000000000000000000000000000000000",
      "difficulty": "0x0",
      "totalDifficulty": "0xe8d4c39480",
      "extraData": "0x",
      "size": "0x3ea",
      "gasLimit": "0x1c9c380",
      "gasUsed": "0x7a122",
      "timestamp": "0x68738af9",
      "transactions": [],
      "uncles": []
    }
  }
}
```
### 2. eth_unsubscribe - 取消订阅
**请求格式**:
```json
{
  "id": 2,
  "method": "eth_unsubscribe",
  "params": ["0x9ce59a13059e417087c02d3236a0b1cc"],
  "jsonrpc": "2.0"
}
```
**响应格式**:
```json
{
  "id": 2,
  "result": true,
  "jsonrpc": "2.0"
}
```
## HTTP JSON-RPC API
### 连接端点
```
POST http://localhost:8090/
POST http://localhost:8090/eth
```
### 1. eth_blockNumber - 获取当前区块号
**请求格式**:
```json
{
  "id": 1,
  "method": "eth_blockNumber",
  "params": [],
  "jsonrpc": "2.0"
}
```
**响应格式**:
```json
{
  "id": 1,
  "result": "0xf4241",
  "jsonrpc": "2.0"
}
```
### 2. eth_getBlockByNumber - 根据区块号获取区块
**请求格式**:
```json
{
  "id": 1,
  "method": "eth_getBlockByNumber",
  "params": ["0xf4241", false],
  "jsonrpc": "2.0"
}
```
**参数说明**:
- `params[0]`: 区块号（十六进制）或特殊值
  - `"latest"` - 最新区块
  - `"earliest"` - 最早区块
  - `"pending"` - 待处理区块
  - `"0xf4241"` - 具体区块号（十六进制）
- `params[1]`: 是否返回完整交易信息（目前固定为false）

**响应格式**:
```json
{
  "id": 1,
  "result": {
    "number": "0xf4241",
    "hash": "0x1234567890abcdef1234567890abcdef12345678",
    "parentHash": "0x0987654321fedcba0987654321fedcba09876543",
    "nonce": "0x0000000000000000",
    "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
    "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
    "stateRoot": "0xabcdef1234567890abcdef1234567890abcdef12",
    "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
    "miner": "0x0000000000000000000000000000000000000000",
    "difficulty": "0x0",
    "totalDifficulty": "0xe8d4c39480",
    "extraData": "0x",
    "size": "0x3ea",
    "gasLimit": "0x1c9c380",
    "gasUsed": "0x7a122",
    "timestamp": "0x68738af9",
    "transactions": [],
    "uncles": []
  },
  "jsonrpc": "2.0"
}
```
# http worker api文档

# kafka文档
## http.tasks
{
	"taskId": "task-12272af70f9944ababadf54995b4f7b3",
	"payload": {
		"dataSourceUrl": "http://localhost:8090",
		"method": "POST",
		"params": {
			"block": "latest",
			"method": "eth_getBlockByNumber",
			"full_tx": "false"
		},
		"apikey": null,
		"headers": {
			"User-Agent": "CryptoDataIngestion/1.0"
		},
		"dataSourceId": "mock-ethereum"
	}
}
## tasks.status
{
	"taskId": "task-12272af70f9944ababadf54995b4f7b3",
	"status": "RETRY",
	"message": "Retryable error: HTTP 429",
	"timestamp": "2025-07-16T13:47:10.818533+08:00",
	"duration": 0,
	"statusCode": 429,
	"dataSize": 61,
	"retryCount": 1
}
