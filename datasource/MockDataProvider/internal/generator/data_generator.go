package generator

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"mock-service/internal/config"
	"mock-service/internal/model"
	"sync"
	"time"
)

// DataGenerator 数据生成器
type DataGenerator struct {
	mu               sync.RWMutex
	currentBlockNumber int64
	lastBlockHash    string
	config           *config.Config
}

// NewDataGenerator 创建新的数据生成器
func NewDataGenerator(cfg *config.Config) *DataGenerator {
	return &DataGenerator{
		currentBlockNumber: cfg.Data.Ethereum.StartBlockNumber,
		lastBlockHash:     generateRandomHash(),
		config:           cfg,
	}
}

// GenerateNextBlock 生成下一个区块头
func (g *DataGenerator) GenerateNextBlock() *model.BlockHeader {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.currentBlockNumber++
	
	// 创建新的区块头
	block := &model.BlockHeader{
		Number:           formatHex(g.currentBlockNumber),
		Hash:             generateRandomHash(),
		ParentHash:       g.lastBlockHash,
		Nonce:            "0x0000000000000000",
		SHA3Uncles:       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		LogsBloom:        "0x" + generateZeros(512), // 256字节的bloom过滤器
		TransactionsRoot: "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		StateRoot:        generateRandomHash(),
		ReceiptsRoot:     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		Miner:            generateRandomAddress(),
		Difficulty:       "0x0", // 以太坊2.0后难度为0
		TotalDifficulty:  formatHex(g.currentBlockNumber * 1000000),
		ExtraData:        "0x",
		Size:             formatHex(1000 + g.currentBlockNumber%500),
		GasLimit:         "0x1c9c380", // 30,000,000 gas
		GasUsed:          formatHex(500000 + g.currentBlockNumber%200000),
		Timestamp:        formatHex(time.Now().Unix()),
		Transactions:     []string{},
		Uncles:           []string{},
	}
	
	// 更新最后区块hash
	g.lastBlockHash = block.Hash
	
	return block
}

// GetCurrentBlockNumber 获取当前区块号
func (g *DataGenerator) GetCurrentBlockNumber() int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.currentBlockNumber
}

// GetBlockByNumber 根据区块号获取区块（用于HTTP补数据）
func (g *DataGenerator) GetBlockByNumber(blockNumber int64) *model.BlockHeader {
	g.mu.RLock()
	defer g.mu.RUnlock()
	
	if blockNumber > g.currentBlockNumber {
		return nil // 区块不存在
	}
	
	// 生成确定性的区块数据（基于区块号）
	return g.generateDeterministicBlock(blockNumber)
}

// generateDeterministicBlock 生成确定性的区块数据
func (g *DataGenerator) generateDeterministicBlock(blockNumber int64) *model.BlockHeader {
	// 使用区块号作为种子生成确定性的哈希
	parentHash := generateDeterministicHash(blockNumber - 1)
	blockHash := generateDeterministicHash(blockNumber)
	
	return &model.BlockHeader{
		Number:           formatHex(blockNumber),
		Hash:             blockHash,
		ParentHash:       parentHash,
		Nonce:            "0x0000000000000000",
		SHA3Uncles:       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		LogsBloom:        "0x" + generateZeros(512),
		TransactionsRoot: "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		StateRoot:        generateDeterministicHash(blockNumber + 1000),
		ReceiptsRoot:     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		Miner:            generateDeterministicAddress(blockNumber),
		Difficulty:       "0x0",
		TotalDifficulty:  formatHex(blockNumber * 1000000),
		ExtraData:        "0x",
		Size:             formatHex(1000 + blockNumber%500),
		GasLimit:         "0x1c9c380",
		GasUsed:          formatHex(500000 + blockNumber%200000),
		Timestamp:        formatHex(time.Now().Unix() - int64(g.currentBlockNumber-blockNumber)*int64(g.config.Data.Ethereum.BlockInterval)),
		Transactions:     []string{},
		Uncles:           []string{},
	}
}

// 生成随机哈希
func generateRandomHash() string {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	return "0x" + hex.EncodeToString(bytes)
}

// 生成确定性哈希
func generateDeterministicHash(seed int64) string {
	// 使用简单的算法生成确定性哈希
	hash := fmt.Sprintf("%064x", seed*31+17)
	return "0x" + hash
}

// 生成随机地址
func generateRandomAddress() string {
	bytes := make([]byte, 20)
	rand.Read(bytes)
	return "0x" + hex.EncodeToString(bytes)
}

// 生成确定性地址
func generateDeterministicAddress(seed int64) string {
	// 使用简单的算法生成确定性地址
	addr := fmt.Sprintf("%040x", seed*13+7)
	return "0x" + addr
}

// 格式化十六进制数字
func formatHex(n int64) string {
	return fmt.Sprintf("0x%x", n)
}

// 生成指定长度的零字符串
func generateZeros(length int) string {
	zeros := make([]byte, length)
	for i := range zeros {
		zeros[i] = '0'
	}
	return string(zeros)
}