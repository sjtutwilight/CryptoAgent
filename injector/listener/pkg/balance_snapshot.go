package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-redis/redis/v8"
	"github.com/sjtutwilight/Twilight/common/pkg/config"
	"github.com/sjtutwilight/Twilight/common/pkg/kafka"
)

// AccountBalance represents account balance snapshot data
type AccountBalance struct {
	AccountID    int64     `json:"account_id"`
	ObservedTime time.Time `json:"observed_time"`
	BlockID      uint64    `json:"block_id"`   // blockchain block number for snapshot
	AssetType    string    `json:"asset_type"` // "ERC20" or "LP"
	BizID        int64     `json:"biz_id"`     // token_id or pair_id
	Amount       string    `json:"amount"`     // balance amount in wei
	PriceUSD     string    `json:"price_usd"`  // USD price
	ValueUSD     string    `json:"value_usd"`  // USD total value
	LabelMask    int       `json:"label_mask"` // bitmap tags

	// Helper fields
	AccountAddress  string `json:"account_address"`
	ContractAddress string `json:"contract_address"`
	BizName         string `json:"biz_name"` // token symbol or pair name
}

// BalanceSnapshotGenerator generates account balance snapshots
type BalanceSnapshotGenerator struct {
	client      *ethclient.Client
	producer    kafka.KafkaProducer
	topic       string
	deployment  *config.DeploymentConfig
	interval    time.Duration
	redisClient *redis.Client

	// Redis price cache (fallback to defaults if Redis unavailable)
	tokenPrices map[string]string // token_address -> price_usd

	// Account metadata cache
	accountMetadata map[string]*AccountInfo // address -> account info
}

// AccountInfo represents account metadata
type AccountInfo struct {
	ID        int64  `json:"id"`
	Address   string `json:"address"`
	Tag       string `json:"tag"`
	TagBitmap int    `json:"tagBitmap"`
}

// TokenInfo represents token metadata
type TokenInfo struct {
	ID       int64  `json:"id"`
	Address  string `json:"address"`
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
}

// PairInfo represents pair metadata
type PairInfo struct {
	ID           int64      `json:"id"`
	Address      string     `json:"address"`
	Token0       *TokenInfo `json:"token0"`
	Token1       *TokenInfo `json:"token1"`
	Token0Symbol string     `json:"token0Symbol"`
	Token1Symbol string     `json:"token1Symbol"`
}

// NewBalanceSnapshotGenerator creates a new balance snapshot generator
func NewBalanceSnapshotGenerator(client *ethclient.Client, producer kafka.KafkaProducer, topic string, deployment *config.DeploymentConfig) *BalanceSnapshotGenerator {
	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis server address
		Password: "",               // No password
		DB:       0,                // Default DB
	})

	return &BalanceSnapshotGenerator{
		client:          client,
		producer:        producer,
		topic:           topic,
		deployment:      deployment,
		interval:        60 * time.Second, // 1 minute interval
		redisClient:     redisClient,
		tokenPrices:     make(map[string]string),
		accountMetadata: make(map[string]*AccountInfo),
	}
}

// Start starts the snapshot generation process
func (g *BalanceSnapshotGenerator) Start(ctx context.Context) error {
	// Initialize caches
	if err := g.initializeCaches(); err != nil {
		return fmt.Errorf("failed to initialize caches: %w", err)
	}

	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	log.Printf("📸 Starting balance snapshot generator with %d minute interval", int(g.interval.Minutes()))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := g.generateSnapshot(ctx); err != nil {
				log.Printf("❌ Error generating snapshot: %v", err)
			}
		}
	}
}

// initializeCaches initializes account metadata and token price caches
func (g *BalanceSnapshotGenerator) initializeCaches() error {
	log.Printf("🔄 Initializing balance snapshot caches...")

	// Initialize account metadata (from deployment config)
	accounts := []AccountInfo{
		{ID: 1, Address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", Tag: "cex", TagBitmap: 1},
		{ID: 2, Address: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8", Tag: "smart_money", TagBitmap: 2},
		{ID: 3, Address: "0x3C44CdDDb6a900fa2b585dd299e03d12FA4293BC", Tag: "whale", TagBitmap: 4},
		{ID: 4, Address: "0x90F79bf6EB2c4f870365E785982E1f101E93b906", Tag: "fresh_wallet", TagBitmap: 16},
		{ID: 5, Address: "0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65", Tag: "normal", TagBitmap: 0},
	}

	for _, account := range accounts {
		g.accountMetadata[strings.ToLower(account.Address)] = &account
	}

	// Initialize token prices (simplified - use default prices)
	g.initializeTokenPrices()

	log.Printf("✅ Initialized %d accounts and %d token prices", len(g.accountMetadata), len(g.tokenPrices))
	return nil
}

// initializeTokenPrices sets up default token prices
func (g *BalanceSnapshotGenerator) initializeTokenPrices() {
	// Default token prices from configuration
	defaultPrices := map[string]string{
		"0x74A6379d012ce53E3b0718C05dD72a3De87F0c6a": "1",      // USDC
		"0x5FbDB2315678afecb367f032d93F642f64180aa3": "3000",   // WETH
		"0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512": "1",      // DAI
		"0x9fE46736679d2D9a65F0992F2272dE9f3c7fa6e0": "50",     // TWI
		"0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9": "120000", // WBTC
	}

	// Convert to lowercase addresses for consistent lookup
	for addr, price := range defaultPrices {
		g.tokenPrices[strings.ToLower(addr)] = price
	}
}

// generateSnapshot generates account balance snapshots for all accounts
func (g *BalanceSnapshotGenerator) generateSnapshot(ctx context.Context) error {
	snapshotTime := time.Now()

	// Get current block number for snapshot
	currentBlock, err := g.client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %w", err)
	}

	log.Printf("📸 Generating account balance snapshot at %s (block %d)", snapshotTime.Format(time.RFC3339), currentBlock)

	var totalSnapshots int

	// Process each account
	for _, account := range g.accountMetadata {
		// Generate ERC20 token snapshots
		erc20Snapshots, err := g.generateERC20Snapshots(ctx, account, snapshotTime, currentBlock)
		if err != nil {
			log.Printf("❌ Error generating ERC20 snapshots for account %s: %v", account.Address, err)
			continue
		}

		// Generate LP token snapshots
		lpSnapshots, err := g.generateLPSnapshots(ctx, account, snapshotTime, currentBlock)
		if err != nil {
			log.Printf("❌ Error generating LP snapshots for account %s: %v", account.Address, err)
			continue
		}

		// Send snapshots to Kafka
		allSnapshots := append(erc20Snapshots, lpSnapshots...)
		for _, snapshot := range allSnapshots {
			if err := g.sendSnapshotToKafka(ctx, snapshot); err != nil {
				log.Printf("❌ Error sending snapshot to Kafka: %v", err)
			} else {
				totalSnapshots++
			}
		}
	}

	log.Printf("✅ Generated %d balance snapshots at %s", totalSnapshots, snapshotTime.Format(time.RFC3339))
	return nil
}

// generateERC20Snapshots generates ERC20 token balance snapshots for an account
func (g *BalanceSnapshotGenerator) generateERC20Snapshots(ctx context.Context, account *AccountInfo, snapshotTime time.Time, blockID uint64) ([]*AccountBalance, error) {
	var snapshots []*AccountBalance

	// Process each token from deployment
	for i, token := range g.deployment.Tokens {
		balance, err := g.getERC20Balance(ctx, token.Address, account.Address)
		if err != nil {
			log.Printf("⚠️ Error getting ERC20 balance for %s on %s: %v", token.Symbol, account.Address, err)
			continue
		}

		// Skip zero balances
		if balance.Cmp(big.NewInt(0)) == 0 {
			continue
		}

		// Convert balance to decimal string (assuming 18 decimals)
		balanceDecimal := new(big.Float).Quo(new(big.Float).SetInt(balance), big.NewFloat(1e18))
		balanceStr := balanceDecimal.Text('f', 18)

		// Get token price
		price := g.getTokenPrice(token.Address)
		priceFloat, _ := strconv.ParseFloat(price, 64)
		balanceFloat, _ := strconv.ParseFloat(balanceStr, 64)
		valueUSD := priceFloat * balanceFloat

		snapshot := &AccountBalance{
			AccountID:       account.ID,
			ObservedTime:    snapshotTime,
			BlockID:         blockID,
			AssetType:       "erc20",
			BizID:           int64(i + 1), // Use index + 1 as token_id
			Amount:          balanceStr,
			PriceUSD:        price,
			ValueUSD:        fmt.Sprintf("%.18f", valueUSD),
			LabelMask:       account.TagBitmap,
			AccountAddress:  account.Address,
			ContractAddress: token.Address,
			BizName:         token.Symbol,
		}

		snapshots = append(snapshots, snapshot)
		log.Printf("📊 ERC20 snapshot: %s holds %.6f %s ($%.2f)", account.Address, balanceFloat, token.Symbol, valueUSD)
	}

	return snapshots, nil
}

// generateLPSnapshots generates LP token balance snapshots for an account
func (g *BalanceSnapshotGenerator) generateLPSnapshots(ctx context.Context, account *AccountInfo, snapshotTime time.Time, blockID uint64) ([]*AccountBalance, error) {
	var snapshots []*AccountBalance

	// Process each pair from deployment
	for i, pair := range g.deployment.Pairs {
		balance, err := g.getERC20Balance(ctx, pair.Address, account.Address)
		if err != nil {
			log.Printf("⚠️ Error getting LP balance for pair %s on %s: %v", pair.Address, account.Address, err)
			continue
		}

		// Skip zero balances
		if balance.Cmp(big.NewInt(0)) == 0 {
			continue
		}

		// Convert balance to decimal string (assuming 18 decimals)
		balanceDecimal := new(big.Float).Quo(new(big.Float).SetInt(balance), big.NewFloat(1e18))
		balanceStr := balanceDecimal.Text('f', 18)

		// Simplified LP price calculation (just use average of token prices)
		lpPrice := g.calculateSimpleLPPrice(pair)
		priceFloat, _ := strconv.ParseFloat(lpPrice, 64)
		balanceFloat, _ := strconv.ParseFloat(balanceStr, 64)
		valueUSD := priceFloat * balanceFloat

		// Get token symbols for pair name
		token0Symbol := g.getTokenSymbol(pair.Token0)
		token1Symbol := g.getTokenSymbol(pair.Token1)
		bizName := fmt.Sprintf("%s-%s LP", token0Symbol, token1Symbol)

		snapshot := &AccountBalance{
			AccountID:       account.ID,
			ObservedTime:    snapshotTime,
			BlockID:         blockID,
			AssetType:       "lp",
			BizID:           int64(i + 1), // Use index + 1 as pair_id
			Amount:          balanceStr,
			PriceUSD:        lpPrice,
			ValueUSD:        fmt.Sprintf("%.18f", valueUSD),
			LabelMask:       account.TagBitmap,
			AccountAddress:  account.Address,
			ContractAddress: pair.Address,
			BizName:         bizName,
		}

		snapshots = append(snapshots, snapshot)
		log.Printf("📊 LP snapshot: %s holds %.6f %s ($%.2f)", account.Address, balanceFloat, bizName, valueUSD)
	}

	return snapshots, nil
}

// getERC20Balance gets the ERC20 token balance for an account
func (g *BalanceSnapshotGenerator) getERC20Balance(ctx context.Context, tokenAddress, accountAddress string) (*big.Int, error) {
	// Simple contract call for balanceOf
	contractAddress := common.HexToAddress(tokenAddress)
	account := common.HexToAddress(accountAddress)

	// balanceOf function signature: 0x70a08231
	data := append([]byte{0x70, 0xa0, 0x82, 0x31}, common.LeftPadBytes(account.Bytes(), 32)...)

	result, err := g.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddress,
		Data: data,
	}, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to call balanceOf: %w", err)
	}

	if len(result) == 0 {
		return big.NewInt(0), nil
	}

	balance := new(big.Int).SetBytes(result)
	return balance, nil
}

// getTokenPrice gets the token price from Redis cache with fallback to defaults
func (g *BalanceSnapshotGenerator) getTokenPrice(tokenAddress string) string {
	address := strings.ToLower(tokenAddress)

	// Try to get price from Redis first
	redisKey := fmt.Sprintf("token_price:%s", address)

	if g.redisClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		price, err := g.redisClient.Get(ctx, redisKey).Result()
		if err == nil && price != "" {
			log.Printf("📈 Got price from Redis for %s: %s", tokenAddress, price)
			return price
		}

		if err != redis.Nil {
			log.Printf("⚠️ Redis error for %s: %v", redisKey, err)
		}
	}

	// Fallback to cached default prices
	if price, exists := g.tokenPrices[address]; exists {
		log.Printf("📊 Using default price for %s: %s", tokenAddress, price)
		return price
	}

	// Final fallback
	log.Printf("⚠️ No price found for %s, using default: 1", tokenAddress)
	return "1"
}

// calculateSimpleLPPrice calculates a simplified LP token price
func (g *BalanceSnapshotGenerator) calculateSimpleLPPrice(pair config.PairConfig) string {
	// Get token prices
	token0Price := g.getTokenPrice(pair.Token0)
	token1Price := g.getTokenPrice(pair.Token1)

	price0, _ := strconv.ParseFloat(token0Price, 64)
	price1, _ := strconv.ParseFloat(token1Price, 64)

	// Simplified calculation: average of token prices * 0.5
	lpPrice := (price0 + price1) / 2 * 0.5

	return fmt.Sprintf("%.18f", lpPrice)
}

// getTokenSymbol gets token symbol by address
func (g *BalanceSnapshotGenerator) getTokenSymbol(tokenAddress string) string {
	for _, token := range g.deployment.Tokens {
		if strings.EqualFold(token.Address, tokenAddress) {
			return token.Symbol
		}
	}
	return "UNKNOWN"
}

// sendSnapshotToKafka sends a balance snapshot to Kafka
func (g *BalanceSnapshotGenerator) sendSnapshotToKafka(ctx context.Context, snapshot *AccountBalance) error {
	// Convert snapshot to JSON
	snapshotJSON, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// Use account_id as key for partitioning
	key := fmt.Sprintf("account_%d", snapshot.AccountID)

	// Send to Kafka
	if err := g.producer.SendMessage(ctx, g.topic, key, string(snapshotJSON)); err != nil {
		return fmt.Errorf("failed to send snapshot to Kafka: %w", err)
	}

	return nil
}
