package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// JSONRPCRequest 代表标准的JSON-RPC请求
//
//	type JSONRPCRequest struct {
//		ID      interface{} `json:"id"`
//		Method  string      `json:"method"`
//		Params  interface{} `json:"params"`
//		JSONRpc string      `json:"jsonrpc"`
//	}
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"` // ← 大小写要完全对齐
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// JSONRPCResponse 代表标准的JSON-RPC响应
type JSONRPCResponse struct {
	ID      interface{}   `json:"id"`
	Result  interface{}   `json:"result,omitempty"`
	Error   *JSONRPCError `json:"error,omitempty"`
	JSONRpc string        `json:"jsonrpc"`
}

// JSONRPCError 代表JSON-RPC错误
type JSONRPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// BlockHeader 代表以太坊区块头
type BlockHeader struct {
	Number           string   `json:"number"`
	Hash             string   `json:"hash"`
	ParentHash       string   `json:"parentHash"`
	Nonce            string   `json:"nonce"`
	SHA3Uncles       string   `json:"sha3Uncles"`
	LogsBloom        string   `json:"logsBloom"`
	TransactionsRoot string   `json:"transactionsRoot"`
	StateRoot        string   `json:"stateRoot"`
	ReceiptsRoot     string   `json:"receiptsRoot"`
	Miner            string   `json:"miner"`
	Difficulty       string   `json:"difficulty"`
	TotalDifficulty  string   `json:"totalDifficulty"`
	ExtraData        string   `json:"extraData"`
	Size             string   `json:"size"`
	GasLimit         string   `json:"gasLimit"`
	GasUsed          string   `json:"gasUsed"`
	Timestamp        string   `json:"timestamp"`
	Transactions     []string `json:"transactions"`
	Uncles           []string `json:"uncles"`
}

// SubscriptionParams 代表订阅参数
type SubscriptionParams struct {
	Type   string        `json:"type"`
	Params []interface{} `json:"params,omitempty"`
}

// SubscriptionResult 代表订阅结果
type SubscriptionResult struct {
	Subscription string      `json:"subscription"`
	Result       interface{} `json:"result"`
}

// NewHeadsNotification 代表新区块头通知
type NewHeadsNotification struct {
	JSONRpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Subscription string      `json:"subscription"`
		Result       BlockHeader `json:"result"`
	} `json:"params"`
}

// 生成区块头的辅助函数
func NewBlockHeader(number int64, parentHash string) *BlockHeader {
	now := time.Now()
	return &BlockHeader{
		Number:           formatHex(number),
		Hash:             generateHash(),
		ParentHash:       parentHash,
		Nonce:            "0x0000000000000000",
		SHA3Uncles:       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		LogsBloom:        "0x" + padZeros(512), // 256字节的bloom过滤器
		TransactionsRoot: "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		StateRoot:        generateHash(),
		ReceiptsRoot:     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		Miner:            "0x0000000000000000000000000000000000000000",
		Difficulty:       "0x0",
		TotalDifficulty:  formatHex(number * 1000000),
		ExtraData:        "0x",
		Size:             formatHex(1000 + number%500),
		GasLimit:         "0x1c9c380",
		GasUsed:          formatHex(500000 + number%200000),
		Timestamp:        formatHex(now.Unix()),
		Transactions:     []string{},
		Uncles:           []string{},
	}
}

// 生成随机哈希值
func generateHash() string {
	return "0x" + padZeros(64)
}

// 格式化十六进制数字
func formatHex(n int64) string {
	return "0x" + padZeros(16)[:16] + fmt.Sprintf("%x", n)
}

// 填充零
func padZeros(length int) string {
	zeros := make([]byte, length)
	for i := range zeros {
		zeros[i] = '0'
	}
	return string(zeros)
}
