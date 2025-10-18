package chain

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/sjtutwilight/Twilight/common/pkg/types"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/spf13/viper"
)

// EventListener handles blockchain event monitoring
type EventListener struct {
	client                *ethclient.Client
	contracts             map[common.Address]*Contract
	eventChan             chan *types.ChainEvent
	errorChan             chan error
	blockDelay            uint64
	blockTime             time.Duration
	pollingInterval       time.Duration
	maxBlocksPerPoll      int
	retryInterval         time.Duration
	requiredConfirmations uint64
	confirmationTimeout   time.Duration
	mu                    sync.Mutex
	lastBlockNumber       uint64
	// Add a transaction cache to prevent duplicate processing
	processedTxs   map[string]bool
	processedTxsMu sync.RWMutex
	// TTL for processed transactions (default: 1 hour)
	processedTxsTTL time.Duration
}

// NewEventListener creates a new event listener
func NewEventListener(client *ethclient.Client, blockDelay uint64) (*EventListener, error) {
	pollingInterval := time.Duration(viper.GetInt("chain.polling.interval_ms")) * time.Millisecond
	if pollingInterval <= 0 {
		pollingInterval = 1 * time.Second // Default polling interval
	}

	maxBlocksPerPoll := viper.GetInt("chain.polling.max_blocks_per_poll")
	if maxBlocksPerPoll <= 0 {
		maxBlocksPerPoll = 100 // Default max blocks per poll
	}

	retryInterval := time.Duration(viper.GetInt("chain.polling.retry_interval_ms")) * time.Millisecond
	if retryInterval <= 0 {
		retryInterval = 5 * time.Second // Default retry interval
	}

	requiredConfirmations := uint64(viper.GetInt("chain.confirmation.required_blocks"))
	confirmationTimeout := time.Duration(viper.GetInt("chain.confirmation.timeout_ms")) * time.Millisecond
	if confirmationTimeout <= 0 {
		confirmationTimeout = 30 * time.Second // Default confirmation timeout
	}

	// Get TTL for processed transactions from config or use default (1 hour)
	processedTxsTTL := time.Duration(viper.GetInt("chain.processed_txs_ttl_ms")) * time.Millisecond
	if processedTxsTTL <= 0 {
		processedTxsTTL = 1 * time.Hour // Default TTL
	}

	return &EventListener{
		client:                client,
		contracts:             make(map[common.Address]*Contract),
		eventChan:             make(chan *types.ChainEvent),
		errorChan:             make(chan error),
		blockDelay:            blockDelay,
		blockTime:             time.Duration(viper.GetInt("chain.block_time_ms")) * time.Millisecond,
		pollingInterval:       pollingInterval,
		maxBlocksPerPoll:      maxBlocksPerPoll,
		retryInterval:         retryInterval,
		requiredConfirmations: requiredConfirmations,
		confirmationTimeout:   confirmationTimeout,
		processedTxs:          make(map[string]bool),
		processedTxsTTL:       processedTxsTTL,
	}, nil
}

// Start begins listening for events
func (l *EventListener) Start(ctx context.Context) error {
	ticker := time.NewTicker(l.pollingInterval)
	defer ticker.Stop()

	// Start a goroutine to clean up old processed transactions
	go l.cleanupProcessedTxs(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := l.pollNewBlocks(ctx); err != nil {
				log.Printf("Error polling blocks: %v, retrying in %v", err, l.retryInterval)
				time.Sleep(l.retryInterval)
				continue
			}
		}
	}
}

// cleanupProcessedTxs periodically removes old processed transactions
func (l *EventListener) cleanupProcessedTxs(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute) // Clean up every 10 minutes
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// In a real implementation, we would track timestamps for each tx
			// and remove based on age. For simplicity, we're just clearing the map.
			l.processedTxsMu.Lock()
			l.processedTxs = make(map[string]bool)
			l.processedTxsMu.Unlock()
			log.Printf("Cleaned up processed transactions cache")
		}
	}
}

// isProcessedTx checks if a transaction has already been processed
func (l *EventListener) isProcessedTx(txHash string) bool {
	l.processedTxsMu.RLock()
	defer l.processedTxsMu.RUnlock()
	return l.processedTxs[txHash]
}

// markTxProcessed marks a transaction as processed
func (l *EventListener) markTxProcessed(txHash string) {
	l.processedTxsMu.Lock()
	defer l.processedTxsMu.Unlock()
	l.processedTxs[txHash] = true
}

func (l *EventListener) pollNewBlocks(ctx context.Context) error {
	l.mu.Lock()
	lastProcessed := l.lastBlockNumber
	l.mu.Unlock()

	currentBlock, err := l.client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %w", err)
	}

	log.Printf("Current block: %d, Last processed block: %d, Max blocks per poll: %d", currentBlock, lastProcessed, l.maxBlocksPerPoll)

	// Ensure we don't process more than maxBlocksPerPoll
	endBlock := currentBlock
	if endBlock > lastProcessed+uint64(l.maxBlocksPerPoll) {
		endBlock = lastProcessed + uint64(l.maxBlocksPerPoll)
	}
	log.Printf("Will process blocks from %d to %d", lastProcessed+1, endBlock)

	for blockNum := lastProcessed + 1; blockNum <= endBlock; blockNum++ {
		if err := l.processBlock(ctx, blockNum); err != nil {
			log.Printf("Failed to process block %d: %v", blockNum, err)
			return fmt.Errorf("failed to process block %d: %w", blockNum, err)
		}
		log.Printf("Successfully processed block %d", blockNum)
	}

	l.mu.Lock()
	l.lastBlockNumber = endBlock
	l.mu.Unlock()
	log.Printf("Updated last processed block to %d", endBlock)

	return nil
}

func (l *EventListener) processBlock(ctx context.Context, blockNum uint64) error {
	// Wait for required confirmations
	if err := l.waitForConfirmations(ctx, blockNum); err != nil {
		return fmt.Errorf("failed to wait for confirmations: %w", err)
	}

	block, err := l.client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
	if err != nil {
		return fmt.Errorf("failed to get block %d: %w", blockNum, err)
	}

	log.Printf("Block %d has %d transactions", blockNum, len(block.Transactions()))

	// Process each transaction in the block
	for _, tx := range block.Transactions() {
		log.Printf("Processing transaction %s", tx.Hash().Hex())
		if err := l.processTransaction(ctx, tx, block, blockNum); err != nil {
			log.Printf("Error processing transaction %s: %v", tx.Hash().Hex(), err)
			continue
		}
	}

	return nil
}

func (l *EventListener) processTransaction(ctx context.Context, tx *ethtypes.Transaction, block *ethtypes.Block, blockNum uint64) error {
	txHash := tx.Hash().Hex()

	// Check if this transaction has already been processed
	if l.isProcessedTx(txHash) {
		log.Printf("Skipping already processed transaction %s", txHash)
		return nil
	}

	receipt, err := l.client.TransactionReceipt(ctx, tx.Hash())
	if err != nil {
		return fmt.Errorf("failed to get transaction receipt: %w", err)
	}

	// Create a single chain event for the transaction
	event := types.NewChainEvent(block, tx, receipt, nil, "")

	// Process logs
	for _, logEntry := range receipt.Logs {
		if contract, exists := l.contracts[logEntry.Address]; exists {
			// Check if this log matches any of our registered event signatures
			if contract.IsInterestedInLog(logEntry) {
				eventName := contract.GetEventName(logEntry.Topics[0])

				// Decode event arguments based on event type
				decodedArgs := make(map[string]interface{})
				switch eventName {
				case "Mint":
					if len(logEntry.Topics) >= 2 && len(logEntry.Data) >= 64 {
						sender := common.HexToAddress(logEntry.Topics[1].Hex())
						amount0 := new(big.Int).SetBytes(logEntry.Data[:32])
						amount1 := new(big.Int).SetBytes(logEntry.Data[32:64])

						decodedArgs = map[string]interface{}{
							"sender":  sender.Hex(),
							"amount0": amount0.String(),
							"amount1": amount1.String(),
						}
					}
				case "Burn":
					if len(logEntry.Topics) >= 2 && len(logEntry.Data) >= 64 {
						sender := common.HexToAddress(logEntry.Topics[1].Hex())
						amount0 := new(big.Int).SetBytes(logEntry.Data[:32])
						amount1 := new(big.Int).SetBytes(logEntry.Data[32:64])

						decodedArgs = map[string]interface{}{
							"sender":  sender.Hex(),
							"amount0": amount0.String(),
							"amount1": amount1.String(),
						}
					}
				case "Sync":
					if len(logEntry.Data) >= 64 {
						reserve0 := new(big.Int).SetBytes(logEntry.Data[:32])
						reserve1 := new(big.Int).SetBytes(logEntry.Data[32:64])

						decodedArgs = map[string]interface{}{
							"reserve0": reserve0.String(),
							"reserve1": reserve1.String(),
						}
					}
				case "Swap":
					if len(logEntry.Data) >= 128 {
						amount0In := new(big.Int).SetBytes(logEntry.Data[:32])
						amount1In := new(big.Int).SetBytes(logEntry.Data[32:64])
						amount0Out := new(big.Int).SetBytes(logEntry.Data[64:96])
						amount1Out := new(big.Int).SetBytes(logEntry.Data[96:128])

						decodedArgs = map[string]interface{}{
							"amount0In":  amount0In.String(),
							"amount1In":  amount1In.String(),
							"amount0Out": amount0Out.String(),
							"amount1Out": amount1Out.String(),
						}
					}
				case "Transfer":
					if len(logEntry.Topics) >= 3 {
						from := common.HexToAddress(logEntry.Topics[1].Hex())
						to := common.HexToAddress(logEntry.Topics[2].Hex())
						value := new(big.Int)
						if len(logEntry.Data) >= 32 {
							value.SetBytes(logEntry.Data[:32])
						}

						decodedArgs = map[string]interface{}{
							"from":  from.Hex(),
							"to":    to.Hex(),
							"value": value.String(),
						}
					}
				}

				// Add this event to the transaction's events array
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
					ContractAddress: logEntry.Address.Hex(),
					LogIndex:        int(logEntry.Index),
					BlockNumber:     int64(logEntry.BlockNumber),
					Topics:          make([]string, len(logEntry.Topics)),
					EventData:       "0x" + hex.EncodeToString(logEntry.Data),
					DecodedArgs:     decodedArgs,
				}

				for i, topic := range logEntry.Topics {
					eventData.Topics[i] = topic.Hex()
				}

				event.Events = append(event.Events, eventData)
			}
		}
	}

	// Only send the event if it has any events
	if len(event.Events) > 0 {
		l.eventChan <- event
		// Mark this transaction as processed
		l.markTxProcessed(txHash)
		log.Printf("Processed transaction %s with %d events", txHash, len(event.Events))
	}

	return nil
}

func (l *EventListener) waitForConfirmations(ctx context.Context, blockNum uint64) error {
	if l.requiredConfirmations == 0 {
		return nil
	}

	deadline := time.Now().Add(l.confirmationTimeout)
	ticker := time.NewTicker(l.pollingInterval)
	defer ticker.Stop()

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("confirmation timeout after %v", l.confirmationTimeout)
		}

		currentBlock, err := l.client.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get current block number: %w", err)
		}

		if currentBlock >= blockNum+l.requiredConfirmations {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			continue
		}
	}
}

// GetEventChan returns the event channel
func (l *EventListener) GetEventChan() <-chan *types.ChainEvent {
	return l.eventChan
}

// GetErrorChan returns the error channel
func (l *EventListener) GetErrorChan() <-chan error {
	return l.errorChan
}

// AddContract adds a contract to monitor
func (l *EventListener) AddContract(address common.Address, contract *Contract) {
	l.contracts[address] = contract
}
