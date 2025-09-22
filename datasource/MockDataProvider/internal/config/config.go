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
	HTTP      HTTPFaultConfig      `mapstructure:"http"`
	WebSocket WebSocketFaultConfig `mapstructure:"websocket"`
}

// HTTPFaultConfig HTTP故障注入配置
type HTTPFaultConfig struct {
	Enabled              bool    `mapstructure:"enabled"`
	FailureProbability   float64 `mapstructure:"failure_probability"`
	RateLimitProbability float64 `mapstructure:"rate_limit_probability"`
	ServerErrorProbability float64 `mapstructure:"server_error_probability"`
}

// WebSocketFaultConfig WebSocket故障注入配置  
type WebSocketFaultConfig struct {
	Enabled                  bool    `mapstructure:"enabled"`
	DisconnectionProbability float64 `mapstructure:"disconnection_probability"`
	DataLossProbability      float64 `mapstructure:"data_loss_probability"`
	HeartbeatAnomalyProbability float64 `mapstructure:"heartbeat_anomaly_probability"`
}

// DataConfig 数据生成配置
type DataConfig struct {
	Ethereum EthereumConfig `mapstructure:"ethereum"`
}

// EthereumConfig 以太坊数据生成配置
type EthereumConfig struct {
	BlockInterval    int   `mapstructure:"block_interval"`    // 区块间隔（秒）
	StartBlockNumber int64 `mapstructure:"start_block_number"` // 起始区块号
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
	
	// 数据生成配置
	viper.SetDefault("data.ethereum.block_interval", 12)
	viper.SetDefault("data.ethereum.start_block_number", 1000000)
}