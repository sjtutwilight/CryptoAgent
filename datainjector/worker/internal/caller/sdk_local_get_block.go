package caller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"

	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func init() {
	Register("sdk_call", func(class string, params map[string]any) (Caller, error) {
		switch class {
		case "LocalGetBlock":
			return newLocalGetBlock(params)
		case "balance_snapshot":
			return newBalanceSnapshotCaller(params)
		default:
			return nil, fmt.Errorf("unknown sdk caller_class: %s", class)
		}
	})
}

type LocalGetBlock struct {
	rpcURL        string
	client        *ethclient.Client
	chainID       *big.Int
	chainIDLabel  string
	confirmations uint64
	maxBlocks     uint64
	blockDelay    uint64

	mu        sync.Mutex
	lastBlock uint64
}

func newLocalGetBlock(params map[string]any) (*LocalGetBlock, error) {
	rpcURL := getString(params, "rpc_endpoint", "")
	if rpcURL == "" {
		return nil, fmt.Errorf("LocalGetBlock: rpc_endpoint required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("LocalGetBlock: dial rpc: %w", err)
	}

	chainID, err := client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("LocalGetBlock: fetch chain id: %w", err)
	}

	chainIDLabel := getString(params, "chain_id", chainID.String())
	confirmations := uint64(getInt(params, "confirmations", 0))
	maxBlocks := uint64(getInt(params, "max_blocks_per_poll", 5))
	if maxBlocks == 0 {
		maxBlocks = 1
	}
	blockDelay := uint64(getInt(params, "block_delay", 0))
	startBlock := uint64(getInt(params, "start_block", 0))

	return &LocalGetBlock{
		rpcURL:        rpcURL,
		client:        client,
		chainID:       chainID,
		chainIDLabel:  chainIDLabel,
		confirmations: confirmations,
		maxBlocks:     maxBlocks,
		blockDelay:    blockDelay,
		lastBlock:     startBlock,
	}, nil
}

func (l *LocalGetBlock) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	l.mu.Lock()
	lastProcessed := l.lastBlock
	l.mu.Unlock()

	latest, err := l.client.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("LocalGetBlock: blockNumber: %w", err)
	}

	if latest <= l.blockDelay {
		return nil, nil
	}
	target := latest - l.blockDelay

	if target <= lastProcessed {
		return nil, nil
	}

	if diff := target - lastProcessed; diff > l.maxBlocks {
		target = lastProcessed + l.maxBlocks
	}

	signer := gethtypes.LatestSignerForChainID(l.chainID)
	var messages []*types.Message

	for blockNum := lastProcessed + 1; blockNum <= target; blockNum++ {
		if latest < blockNum+l.confirmations {
			break
		}
		block, err := l.client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
		if err != nil {
			return nil, fmt.Errorf("LocalGetBlock: block %d: %w", blockNum, err)
		}
		blockTimeMs := int64(block.Time()) * 1000
		blockHash := block.Hash().Hex()

		for _, tx := range block.Transactions() {
			fromAddr, err := gethtypes.Sender(signer, tx)
			if err != nil {
				continue
			}
			toAddr := ""
			if tx.To() != nil {
				toAddr = tx.To().Hex()
			}
			receipt, err := l.client.TransactionReceipt(ctx, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("LocalGetBlock: receipt %s: %w", tx.Hash().Hex(), err)
			}

			raw := l.buildRaw(blockNum, blockHash, blockTimeMs, tx, fromAddr.Hex(), toAddr, receipt)
			payload, err := json.Marshal(raw)
			if err != nil {
				continue
			}
			msg := &types.Message{
				Metadata: map[string]any{
					"chain_id":     l.chainIDLabel,
					"block_number": blockNum,
					"tx_hash":      raw.TxHash,
				},
				Payload: payload,
			}
			messages = append(messages, msg)
		}
		l.setLastBlock(blockNum)
	}

	return messages, nil
}

func (l *LocalGetBlock) setLastBlock(blockNum uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if blockNum > l.lastBlock {
		l.lastBlock = blockNum
	}
}

func gasPrice(tx *gethtypes.Transaction) string {
	if tx.Type() == gethtypes.DynamicFeeTxType {
		if tx.GasFeeCap() != nil {
			return tx.GasFeeCap().String()
		}
	}
	if tx.GasPrice() != nil {
		return tx.GasPrice().String()
	}
	return "0"
}

func getString(m map[string]any, key, def string) string {
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case string:
			return vv
		}
	}
	return def
}

func getInt(m map[string]any, key string, def int) int {
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case int:
			return vv
		case int64:
			return int(vv)
		case float64:
			return int(vv)
		case uint64:
			return int(vv)
		}
	}
	return def
}

type rawLog struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	Index       uint     `json:"index"`
	BlockNumber uint64   `json:"block_number"`
}

type rawTransaction struct {
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

func (l *LocalGetBlock) buildRaw(blockNum uint64, blockHash string, timestampMs int64, tx *gethtypes.Transaction, from, to string, receipt *gethtypes.Receipt) *rawTransaction {
	logs := make([]rawLog, 0, len(receipt.Logs))
	for _, logEntry := range receipt.Logs {
		topics := make([]string, len(logEntry.Topics))
		for i, topic := range logEntry.Topics {
			topics[i] = topic.Hex()
		}
		logs = append(logs, rawLog{
			Address:     logEntry.Address.Hex(),
			Topics:      topics,
			Data:        "0x" + hex.EncodeToString(logEntry.Data),
			Index:       uint(logEntry.Index),
			BlockNumber: logEntry.BlockNumber,
		})
	}

	status := "failure"
	if receipt.Status == 1 {
		status = "success"
	}

	return &rawTransaction{
		ChainID:           l.chainIDLabel,
		BlockNumber:       blockNum,
		BlockHash:         blockHash,
		Timestamp:         timestampMs,
		TxHash:            tx.Hash().Hex(),
		TxIndex:           uint(receipt.TransactionIndex),
		Status:            status,
		GasUsed:           receipt.GasUsed,
		GasPrice:          gasPrice(tx),
		Nonce:             tx.Nonce(),
		From:              from,
		To:                to,
		Value:             tx.Value().String(),
		Input:             "0x" + hex.EncodeToString(tx.Data()),
		ContractAddress:   receipt.ContractAddress.Hex(),
		CumulativeGasUsed: receipt.CumulativeGasUsed,
		Logs:              logs,
	}
}
