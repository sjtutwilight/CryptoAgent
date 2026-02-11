package generator

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"mock-service/internal/config"
	"mock-service/internal/model"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DataGenerator 数据生成器
type DataGenerator struct {
	mu                 sync.RWMutex
	currentBlockNumber int64
	lastBlockHash      string
	config             *config.Config
	stopChan           chan struct{} // 用于停止后台保存
	saveDone           chan struct{} // 用于等待保存完成
}

// BlockState 区块状态(用于持久化)
type BlockState struct {
	BlockNumber   int64  `json:"block_number"`
	LastBlockHash string `json:"last_block_hash"`
	Timestamp     int64  `json:"timestamp"`
}

// NewDataGenerator 创建新的数据生成器
func NewDataGenerator(cfg *config.Config) *DataGenerator {
	g := &DataGenerator{
		currentBlockNumber: cfg.Data.Ethereum.StartBlockNumber,
		lastBlockHash:      generateRandomHash(),
		config:             cfg,
		stopChan:           make(chan struct{}),
		saveDone:           make(chan struct{}),
	}

	// 如果启用了持久化，尝试从文件恢复状态
	if cfg.Data.Ethereum.Persistence.Enabled {
		g.loadState()
	}

	// 启动后台保存任务
	if cfg.Data.Ethereum.Persistence.Enabled && cfg.Data.Ethereum.Persistence.SaveInterval > 0 {
		go g.periodicSave()
	}

	return g
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

// ReorgChain 执行链重组,回退指定数量的区块
func (g *DataGenerator) ReorgChain(reorgDepth int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if reorgDepth <= 0 {
		return
	}

	// 回退区块号
	oldBlockNumber := g.currentBlockNumber
	g.currentBlockNumber -= int64(reorgDepth)

	// 确保不会回退到起始区块之前
	if g.currentBlockNumber < g.config.Data.Ethereum.StartBlockNumber {
		g.currentBlockNumber = g.config.Data.Ethereum.StartBlockNumber
	}

	// 重新生成lastBlockHash(模拟新的分叉)
	g.lastBlockHash = generateDeterministicHash(g.currentBlockNumber)

	log.Printf("[链重组] 发生链重组: 回退 %d 个区块, %d -> %d", reorgDepth, oldBlockNumber, g.currentBlockNumber)
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

// GenerateBlock 生成指定区块号的区块（WebSocket补数据使用）
func (g *DataGenerator) GenerateBlock(blockNumber int64) *model.BlockHeader {
	return g.GetBlockByNumber(blockNumber)
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

// loadState 从文件加载状态
func (g *DataGenerator) loadState() {
	stateFile := g.config.Data.Ethereum.Persistence.StateFile

	// 读取状态文件
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[持久化] 状态文件不存在，使用初始配置: block_number=%d", g.config.Data.Ethereum.StartBlockNumber)
			return
		}
		log.Printf("[持久化] 读取状态文件失败: %v，使用初始配置", err)
		return
	}

	// 解析状态
	var state BlockState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("[持久化] 解析状态文件失败: %v，使用初始配置", err)
		return
	}

	// 恢复状态
	recoveredBlock := state.BlockNumber

	// 如果启用了宕机模拟，增加丢失的区块数
	if g.config.Data.Ethereum.CrashSimulation.Enabled {
		lostBlocks := g.config.Data.Ethereum.CrashSimulation.LostBlocks
		recoveredBlock += lostBlocks
		log.Printf("[宕机模拟] 启用宕机模拟，丢失 %d 个区块", lostBlocks)
		log.Printf("[宕机模拟] 原区块号: %d -> 新区块号: %d", state.BlockNumber, recoveredBlock)
	}

	g.currentBlockNumber = recoveredBlock
	g.lastBlockHash = state.LastBlockHash

	elapsedTime := time.Since(time.Unix(state.Timestamp, 0))
	log.Printf("[持久化] 成功恢复状态: block_number=%d, elapsed_time=%v", recoveredBlock, elapsedTime)
}

// saveState 保存当前状态到文件
func (g *DataGenerator) saveState() error {
	g.mu.RLock()
	state := BlockState{
		BlockNumber:   g.currentBlockNumber,
		LastBlockHash: g.lastBlockHash,
		Timestamp:     time.Now().Unix(),
	}
	g.mu.RUnlock()

	// 序列化状态
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化状态失败: %w", err)
	}

	stateFile := g.config.Data.Ethereum.Persistence.StateFile

	// 确保目录存在
	dir := filepath.Dir(stateFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建状态目录失败: %w", err)
	}

	// 写入临时文件，然后重命名（原子操作）
	tempFile := stateFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("写入临时文件失败: %w", err)
	}

	if err := os.Rename(tempFile, stateFile); err != nil {
		return fmt.Errorf("重命名状态文件失败: %w", err)
	}

	return nil
}

// periodicSave 定期保存状态
func (g *DataGenerator) periodicSave() {
	ticker := time.NewTicker(time.Duration(g.config.Data.Ethereum.Persistence.SaveInterval) * time.Second)
	defer ticker.Stop()
	defer close(g.saveDone)

	log.Printf("[持久化] 启动定期保存任务，间隔: %d秒", g.config.Data.Ethereum.Persistence.SaveInterval)

	for {
		select {
		case <-ticker.C:
			if err := g.saveState(); err != nil {
				log.Printf("[持久化] 保存状态失败: %v", err)
			} else {
				g.mu.RLock()
				blockNum := g.currentBlockNumber
				g.mu.RUnlock()
				log.Printf("[持久化] 状态已保存: block_number=%d", blockNum)
			}
		case <-g.stopChan:
			// 收到停止信号，执行最后一次保存
			log.Println("[持久化] 收到停止信号，执行最后一次保存...")
			if err := g.saveState(); err != nil {
				log.Printf("[持久化] 最后保存失败: %v", err)
			} else {
				g.mu.RLock()
				blockNum := g.currentBlockNumber
				g.mu.RUnlock()
				log.Printf("[持久化] 最后保存成功: block_number=%d", blockNum)
			}
			return
		}
	}
}

// Stop 停止数据生成器（优雅关闭）
func (g *DataGenerator) Stop() {
	if g.config.Data.Ethereum.Persistence.Enabled && g.config.Data.Ethereum.Persistence.SaveInterval > 0 {
		log.Println("[持久化] 停止数据生成器...")
		close(g.stopChan)

		// 等待保存完成，最多等待5秒
		select {
		case <-g.saveDone:
			log.Println("[持久化] 数据生成器已优雅停止")
		case <-time.After(5 * time.Second):
			log.Println("[持久化] 等待保存超时")
		}
	}
}
