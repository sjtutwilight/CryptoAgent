package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// Config 应用程序配置
type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Fault  FaultConfig  `mapstructure:"fault"`
	Data   DataConfig   `mapstructure:"data"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

// FaultConfig 故障注入配置
type FaultConfig struct {
	HTTP       HTTPFaultConfig       `mapstructure:"http"`
	WebSocket  WebSocketFaultConfig  `mapstructure:"websocket"`
	ChainReorg ChainReorgFaultConfig `mapstructure:"chain_reorg"`
}

// HTTPFaultConfig HTTP故障注入配置
type HTTPFaultConfig struct {
	Enabled                bool    `mapstructure:"enabled"`
	FailureProbability     float64 `mapstructure:"failure_probability"`
	RateLimitProbability   float64 `mapstructure:"rate_limit_probability"`
	ServerErrorProbability float64 `mapstructure:"server_error_probability"`
}

// WebSocketFaultConfig WebSocket故障注入配置
type WebSocketFaultConfig struct {
	Enabled                     bool    `mapstructure:"enabled"`
	DisconnectionProbability    float64 `mapstructure:"disconnection_probability"`
	DataLossProbability         float64 `mapstructure:"data_loss_probability"`
	HeartbeatAnomalyProbability float64 `mapstructure:"heartbeat_anomaly_probability"`
}

// ChainReorgFaultConfig 链重组故障注入配置
type ChainReorgFaultConfig struct {
	Enabled       bool    `mapstructure:"enabled"`         // 是否启用链重组模拟
	Probability   float64 `mapstructure:"probability"`     // 链重组发生概率
	ReorgDepthMin int     `mapstructure:"reorg_depth_min"` // 最小回退区块数
	ReorgDepthMax int     `mapstructure:"reorg_depth_max"` // 最大回退区块数
}

// DataConfig 数据生成配置
type DataConfig struct {
	Ethereum EthereumConfig `mapstructure:"ethereum"`
	Binance  BinanceConfig  `mapstructure:"binance"`
}

// EthereumConfig 以太坊数据生成配置
type EthereumConfig struct {
	BlockInterval    int                   `mapstructure:"block_interval"`     // 区块间隔（秒）
	StartBlockNumber int64                 `mapstructure:"start_block_number"` // 起始区块号
	Persistence      PersistenceConfig     `mapstructure:"persistence"`        // 持久化配置
	CrashSimulation  CrashSimulationConfig `mapstructure:"crash_simulation"`   // 宕机模拟配置
}

// PersistenceConfig 持久化配置
type PersistenceConfig struct {
	Enabled      bool   `mapstructure:"enabled"`       // 是否启用持久化
	StateFile    string `mapstructure:"state_file"`    // 状态文件路径
	SaveInterval int    `mapstructure:"save_interval"` // 保存间隔（秒）
}

// CrashSimulationConfig 宕机模拟配置
type CrashSimulationConfig struct {
	Enabled    bool  `mapstructure:"enabled"`     // 是否启用宕机模拟
	LostBlocks int64 `mapstructure:"lost_blocks"` // 宕机时丢失的区块数量
}

// BinanceConfig Binance订单簿模拟配置
type BinanceConfig struct {
	Enabled       bool                  `mapstructure:"enabled"`
	IntervalMs    int                   `mapstructure:"interval_ms"`
	SnapshotDepth int                   `mapstructure:"snapshot_depth"`
	Symbols       []BinanceSymbolConfig `mapstructure:"symbols"`
}

// BinanceSymbolConfig 单个交易对配置
type BinanceSymbolConfig struct {
	Symbol       string  `mapstructure:"symbol"`
	BasePrice    float64 `mapstructure:"base_price"`
	PriceTick    float64 `mapstructure:"price_tick"`
	QuantityTick float64 `mapstructure:"quantity_tick"`
	Levels       int     `mapstructure:"levels"`
}

var globalConfig *Config

// Load 加载配置文件
func Load(configPath string) error {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	// 设置默认值
	setDefaults()

	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return fmt.Errorf("解析配置文件失败: %w", err)
	}

	globalConfig = &config
	return nil
}

// Get 获取全局配置
func Get() *Config {
	return globalConfig
}

// setDefaults 设置默认配置值
func setDefaults() {
	// 服务器配置
	viper.SetDefault("server.host", "localhost")
	viper.SetDefault("server.port", 8080)

	// 故障注入配置
	viper.SetDefault("fault.http.enabled", true)
	viper.SetDefault("fault.http.failure_probability", 0.05)
	viper.SetDefault("fault.http.rate_limit_probability", 0.02)
	viper.SetDefault("fault.http.server_error_probability", 0.03)

	viper.SetDefault("fault.websocket.enabled", true)
	viper.SetDefault("fault.websocket.disconnection_probability", 0.01)
	viper.SetDefault("fault.websocket.data_loss_probability", 0.02)
	viper.SetDefault("fault.websocket.heartbeat_anomaly_probability", 0.01)

	// 链重组故障注入配置
	viper.SetDefault("fault.chain_reorg.enabled", false)
	viper.SetDefault("fault.chain_reorg.probability", 0.001)
	viper.SetDefault("fault.chain_reorg.reorg_depth_min", 1)
	viper.SetDefault("fault.chain_reorg.reorg_depth_max", 5)

	// 数据生成配置
	viper.SetDefault("data.ethereum.block_interval", 12)
	viper.SetDefault("data.ethereum.start_block_number", 1000000)
	viper.SetDefault("data.binance.enabled", false)
	viper.SetDefault("data.binance.interval_ms", 200)
	viper.SetDefault("data.binance.snapshot_depth", 200)

	// 持久化配置
	viper.SetDefault("data.ethereum.persistence.enabled", true)
	viper.SetDefault("data.ethereum.persistence.state_file", "./data/block_state.json")
	viper.SetDefault("data.ethereum.persistence.save_interval", 10)

	// 宕机模拟配置
	viper.SetDefault("data.ethereum.crash_simulation.enabled", false)
	viper.SetDefault("data.ethereum.crash_simulation.lost_blocks", 0)
}
