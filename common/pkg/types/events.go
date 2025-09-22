package types

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// ChainEvent represents a blockchain event with transaction and event details
type ChainEvent struct {
	Transaction struct {
		BlockNumber       int64  `json:"blockNumber"`
		BlockHash         string `json:"blockHash"`
		Timestamp         int64  `json:"timestamp"`
		TransactionHash   string `json:"transactionHash"`
		TransactionIndex  int    `json:"transactionIndex"`
		TransactionStatus string `json:"transactionStatus"`
		GasUsed           int64  `json:"gasUsed"`
		GasPrice          string `json:"gasPrice"`
		Nonce             uint64 `json:"nonce"`
		FromAddress       string `json:"fromAddress"`
		ToAddress         string `json:"toAddress"`
		TransactionValue  string `json:"transactionValue"`
		InputData         string `json:"inputData"`
		ChainID           string `json:"chainID"`
	} `json:"transaction"`

	Events []struct {
		EventName       string                 `json:"eventName"`
		ContractAddress string                 `json:"contractAddress"`
		LogIndex        int                    `json:"logIndex"`
		BlockNumber     int64                  `json:"blockNumber"`
		Topics          []string               `json:"topics"`
		EventData       string                 `json:"eventData"`
		DecodedArgs     map[string]interface{} `json:"decodedArgs"`
	} `json:"events"`
}

// NewChainEvent creates a new ChainEvent from block, transaction, receipt and log data
func NewChainEvent(block *types.Block, tx *types.Transaction, receipt *types.Receipt, log *types.Log, eventName string) *ChainEvent {
	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		from = common.Address{}
	}

	var to string
	if tx.To() != nil {
		to = tx.To().Hex()
	}

	event := &ChainEvent{}
	event.Transaction.BlockNumber = block.Number().Int64()
	event.Transaction.BlockHash = block.Hash().Hex()

	event.Transaction.Timestamp = int64(time.Now().UnixMilli())
	event.Transaction.TransactionHash = tx.Hash().Hex()
	event.Transaction.TransactionIndex = int(receipt.TransactionIndex)
	event.Transaction.TransactionStatus = getTransactionStatus(receipt.Status)
	event.Transaction.GasUsed = int64(receipt.GasUsed)
	event.Transaction.GasPrice = tx.GasPrice().String()
	event.Transaction.Nonce = tx.Nonce()
	event.Transaction.FromAddress = from.Hex()
	event.Transaction.ToAddress = to
	event.Transaction.TransactionValue = tx.Value().String()
	event.Transaction.InputData = "0x" + hex.EncodeToString(tx.Data())
	event.Transaction.ChainID = tx.ChainId().String()

	if log != nil {
		eventData := struct {
			EventName       string                 `json:"eventName"`
			ContractAddress string                 `json:"contractAddress"`
			LogIndex        int                    `json:"logIndex"`
			BlockNumber     int64                  `json:"blockNumber"`
			Topics          []string               `json:"topics"`
			EventData       string                 `json:"eventData"`
			DecodedArgs     map[string]interface{} `json:"decodedArgs"`
		}{
			EventName:       eventName,
			ContractAddress: log.Address.Hex(),
			LogIndex:        int(log.Index),
			BlockNumber:     int64(log.BlockNumber),
			Topics:          make([]string, len(log.Topics)),
			EventData:       "0x" + hex.EncodeToString(log.Data),
			DecodedArgs:     make(map[string]interface{}),
		}

		for i, topic := range log.Topics {
			eventData.Topics[i] = topic.Hex()
		}

		event.Events = append(event.Events, eventData)
	}

	return event
}

func getTransactionStatus(status uint64) string {
	if status == 1 {
		return "success"
	}
	return "failed"
}

// DecodedEvent represents a decoded event with its arguments
type DecodedEvent struct {
	Args map[string]interface{}
}

// DecodeEvent decodes the event data string into a DecodedEvent
func DecodeEvent(eventData string) (*DecodedEvent, error) {
	// Remove "0x" prefix if present
	if len(eventData) > 2 && eventData[:2] == "0x" {
		eventData = eventData[2:]
	}

	// Decode hex string to bytes
	data, err := hex.DecodeString(eventData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex string: %w", err)
	}

	// For now, we'll return a simple map with the raw data
	// In production, this should properly decode based on the event ABI
	return &DecodedEvent{
		Args: map[string]interface{}{
			"data": data,
		},
	}, nil
}
