package parser

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

const (
	topicTransfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	topicSync     = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
	topicSwap     = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
	topicMint     = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
	topicBurn     = "0x7a530d0d04921d181a098970b041fb8ea6cb87d6d5c27446b109bd4d1b61c02f"
)

// DexParser 解析 DEX (去中心化交易所) 交易日志
// 提取并解析 EVM 链上的 Transfer, Sync, Swap, Mint, Burn 等事件
type DexParser struct {
	ChainID string
}

type rawLog struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	Index       uint     `json:"index"`
	BlockNumber uint64   `json:"block_number"`
}

type rawTx struct {
	ChainID           string   `json:"chain_id"`
	BlockNumber       uint64   `json:"block_number"`
	BlockHash         string   `json:"block_hash"`
	Timestamp         int64    `json:"timestamp_ms"`
	TxHash            string   `json:"tx_hash"`
	TxIndex           uint     `json:"tx_index"`
	Status            string   `json:"status"`
	GasUsed           uint64   `json:"gas_used"`
	GasPrice          string   `json:"gas_price"`
	Nonce             uint64   `json:"nonce"`
	From              string   `json:"from"`
	To                string   `json:"to"`
	Value             string   `json:"value"`
	Input             string   `json:"input"`
	ContractAddress   string   `json:"contract_address"`
	CumulativeGasUsed uint64   `json:"cumulative_gas_used"`
	Logs              []rawLog `json:"logs"`
}

type transactionPayload struct {
	BlockNumber       uint64 `json:"blockNumber"`
	BlockHash         string `json:"blockHash"`
	Timestamp         int64  `json:"timestamp"`
	TransactionHash   string `json:"transactionHash"`
	TransactionIndex  uint   `json:"transactionIndex"`
	TransactionStatus string `json:"transactionStatus"`
	GasUsed           uint64 `json:"gasUsed"`
	GasPrice          string `json:"gasPrice"`
	Nonce             uint64 `json:"nonce"`
	FromAddress       string `json:"fromAddress"`
	ToAddress         string `json:"toAddress"`
	TransactionValue  string `json:"transactionValue"`
	InputData         string `json:"inputData"`
	ChainID           string `json:"chainID"`
}

type eventPayload struct {
	EventName       string                 `json:"eventName"`
	ContractAddress string                 `json:"contractAddress"`
	LogIndex        uint                   `json:"logIndex"`
	BlockNumber     uint64                 `json:"blockNumber"`
	Topics          []string               `json:"topics"`
	EventData       string                 `json:"eventData"`
	DecodedArgs     map[string]interface{} `json:"decodedArgs,omitempty"`
}

type outputPayload struct {
	Transaction transactionPayload `json:"transaction"`
	Events      []eventPayload     `json:"events"`
}

// NewDexParser 创建 DEX 解析器
func NewDexParser(cfg map[string]any) (*DexParser, error) {
	chainID := ""
	if cfg != nil {
		if v, ok := cfg["chain_id"].(string); ok {
			chainID = v
		}
	}
	return &DexParser{ChainID: chainID}, nil
}

// Handle 处理交易日志，提取并解析 DEX 相关事件
func (d *DexParser) Handle(msg *types.Message) ([]*types.Message, error) {
	var raw rawTx
	if err := json.Unmarshal(msg.Payload, &raw); err != nil {
		return nil, fmt.Errorf("dex_parser: invalid payload: %w", err)
	}

	chainID := coalesce(raw.ChainID, d.ChainID)
	if chainID == "" {
		chainID = "unknown"
	}

	if len(raw.Logs) == 0 {
		return nil, nil
	}

	events := make([]eventPayload, 0, len(raw.Logs))
	for _, lg := range raw.Logs {
		if len(lg.Topics) == 0 {
			continue
		}
		eventName := eventNameFromTopic(lg.Topics[0])
		decoded := decodeArgs(eventName, lg.Topics, lg.Data)
		if eventName == "" && len(decoded) == 0 {
			continue
		}
		events = append(events, eventPayload{
			EventName:       eventName,
			ContractAddress: lg.Address,
			LogIndex:        lg.Index,
			BlockNumber:     lg.BlockNumber,
			Topics:          lg.Topics,
			EventData:       lg.Data,
			DecodedArgs:     decoded,
		})
	}

	if len(events) == 0 {
		return nil, nil
	}

	out := outputPayload{
		Transaction: transactionPayload{
			BlockNumber:       raw.BlockNumber,
			BlockHash:         raw.BlockHash,
			Timestamp:         raw.Timestamp,
			TransactionHash:   raw.TxHash,
			TransactionIndex:  raw.TxIndex,
			TransactionStatus: raw.Status,
			GasUsed:           raw.GasUsed,
			GasPrice:          raw.GasPrice,
			Nonce:             raw.Nonce,
			FromAddress:       raw.From,
			ToAddress:         raw.To,
			TransactionValue:  raw.Value,
			InputData:         raw.Input,
			ChainID:           chainID,
		},
		Events: events,
	}

	payload, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}

	msg.Metadata = map[string]any{
		"chain_id":     chainID,
		"tx_hash":      raw.TxHash,
		"block_number": raw.BlockNumber,
	}
	msg.Payload = payload
	return []*types.Message{msg}, nil
}

// coalesce 返回第一个非空字符串
func coalesce(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// eventNameFromTopic 根据 topic hash 识别事件名称
func eventNameFromTopic(topic string) string {
	switch strings.ToLower(topic) {
	case topicTransfer:
		return "Transfer"
	case topicSync:
		return "Sync"
	case topicSwap:
		return "Swap"
	case topicMint:
		return "Mint"
	case topicBurn:
		return "Burn"
	default:
		return ""
	}
}

// decodeArgs 根据事件类型解析参数
func decodeArgs(eventName string, topics []string, data string) map[string]interface{} {
	dataBytes := decodeHex(data)
	if eventName == "" {
		return nil
	}
	result := make(map[string]interface{})
	switch eventName {
	case "Transfer":
		if len(topics) >= 3 {
			result["from"] = toAddress(topics[1])
			result["to"] = toAddress(topics[2])
		}
		if len(dataBytes) >= 32 {
			value := new(big.Int).SetBytes(dataBytes[:32])
			result["value"] = value.String()
		}
	case "Sync":
		if len(dataBytes) >= 64 {
			reserve0 := new(big.Int).SetBytes(dataBytes[:32])
			reserve1 := new(big.Int).SetBytes(dataBytes[32:64])
			result["reserve0"] = reserve0.String()
			result["reserve1"] = reserve1.String()
		}
	case "Swap":
		if len(dataBytes) >= 128 {
			amount0In := new(big.Int).SetBytes(dataBytes[:32])
			amount1In := new(big.Int).SetBytes(dataBytes[32:64])
			amount0Out := new(big.Int).SetBytes(dataBytes[64:96])
			amount1Out := new(big.Int).SetBytes(dataBytes[96:128])
			result["amount0In"] = amount0In.String()
			result["amount1In"] = amount1In.String()
			result["amount0Out"] = amount0Out.String()
			result["amount1Out"] = amount1Out.String()
		}
	case "Mint", "Burn":
		if len(topics) >= 2 {
			result["sender"] = toAddress(topics[1])
		}
		if len(dataBytes) >= 64 {
			amount0 := new(big.Int).SetBytes(dataBytes[:32])
			amount1 := new(big.Int).SetBytes(dataBytes[32:64])
			result["amount0"] = amount0.String()
			result["amount1"] = amount1.String()
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// decodeHex 解码十六进制字符串
func decodeHex(input string) []byte {
	trim := strings.TrimPrefix(strings.ToLower(input), "0x")
	if trim == "" {
		return nil
	}
	b, err := hex.DecodeString(trim)
	if err != nil {
		return nil
	}
	return b
}

// toAddress 从 topic 中提取地址（去除左侧填充的 0）
func toAddress(topic string) string {
	clean := strings.TrimPrefix(strings.ToLower(topic), "0x")
	if len(clean) >= 40 {
		return "0x" + clean[len(clean)-40:]
	}
	return topic
}






