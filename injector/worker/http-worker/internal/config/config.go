package config

import (
	"fmt"
	"github.com/spf13/viper"
)

// Config 应用程序配置
type Config struct {
	Server        ServerConfig              `mapstructure:"server"`
	Kafka         KafkaConfig               `mapstructure:"kafka"`
	HTTP          HTTPConfig                `mapstructure:"http"`
	RateLimit     RateLimitConfig           `mapstructure:"rate_limit"`
	DataSources   map[string]DataSourceConfig `mapstructure:"datasources"`
	Logging       LoggingConfig             `mapstructure:"logging"`
	TopicMapping  TopicMappingConfig        `mapstructure:"topic_mapping"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port int `mapstructure:"port"`
}

// KafkaConfig Kafka配置
type KafkaConfig struct {
	Brokers  []string        `mapstructure:"brokers"`
	Consumer ConsumerConfig  `mapstructure:"consumer"`
	Producer ProducerConfig  `mapstructure:"producer"`
}

// ConsumerConfig Kafka消费者配置
type ConsumerConfig struct {
	GroupID           string            `mapstructure:"group_id"`
	Topics            map[string]string `mapstructure:"topics"`
	AutoOffsetReset   string            `mapstructure:"auto_offset_reset"`
	SessionTimeout    int               `mapstructure:"session_timeout"`
	HeartbeatInterval int               `mapstructure:"heartbeat_interval"`
}

// ProducerConfig Kafka生产者配置
type ProducerConfig struct {
	Retries int    `mapstructure:"retries"`
	Acks    string `mapstructure:"acks"`
	Timeout int    `mapstructure:"timeout"`
}

// HTTPConfig HTTP客户端配置
type HTTPConfig struct {
	Client HTTPClientConfig `mapstructure:"client"`
}

// HTTPClientConfig HTTP客户端详细配置
type HTTPClientConfig struct {
	Timeout            int `mapstructure:"timeout"`
	ConnectionTimeout  int `mapstructure:"connection_timeout"`
	MaxIdleConns       int `mapstructure:"max_idle_conns"`
	MaxConnsPerHost    int `mapstructure:"max_conns_per_host"`
	IdleConnTimeout    int `mapstructure:"idle_conn_timeout"`
	KeepAlive          int `mapstructure:"keep_alive"`
}

// RateLimitConfig 限流配置
type RateLimitConfig struct {
	RefillIntervalMs   int `mapstructure:"refill_interval_ms"`
	DefaultCapacity    int `mapstructure:"default_capacity"`
	DefaultRefillRate  int `mapstructure:"default_refill_rate"`
}

// DataSourceConfig 数据源配置
type DataSourceConfig struct {
	URL       string          `mapstructure:"url"`
	RateLimit DataSourceRateLimit `mapstructure:"rate_limit"`
}

// DataSourceRateLimit 数据源限流配置
type DataSourceRateLimit struct {
	Interval int    `mapstructure:"interval"`
	Weight   int    `mapstructure:"weight"`
	CostRule string `mapstructure:"cost_rule"`
}

// LoggingConfig 日志配置
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// TopicMappingConfig Topic映射配置
type TopicMappingConfig struct {
	DataTopicPrefix   string            `mapstructure:"data_topic_prefix"`
	StatusTopic       string            `mapstructure:"status_topic"`
	DataSourceTopics  map[string]string `mapstructure:"datasource_topics"`
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

// GetDataSourceTopic 获取数据源对应的topic
func (c *Config) GetDataSourceTopic(dataSourceID string) string {
	if topic, exists := c.TopicMapping.DataSourceTopics[dataSourceID]; exists {
		return topic
	}
	// 如果没有配置映射，使用默认规则
	return c.TopicMapping.DataTopicPrefix + dataSourceID
}

// setDefaults 设置默认配置值
func setDefaults() {
	// 服务器配置
	viper.SetDefault("server.port", 8081)
	
	// Kafka配置
	viper.SetDefault("kafka.brokers", []string{"localhost:9092"})
	viper.SetDefault("kafka.consumer.group_id", "http-worker-group")
	viper.SetDefault("kafka.consumer.auto_offset_reset", "earliest")
	viper.SetDefault("kafka.consumer.session_timeout", 30000)
	viper.SetDefault("kafka.consumer.heartbeat_interval", 3000)
	viper.SetDefault("kafka.producer.retries", 3)
	viper.SetDefault("kafka.producer.acks", "all")
	viper.SetDefault("kafka.producer.timeout", 5000)
	
	// HTTP客户端配置
	viper.SetDefault("http.client.timeout", 10)
	viper.SetDefault("http.client.connection_timeout", 5)
	viper.SetDefault("http.client.max_idle_conns", 100)
	viper.SetDefault("http.client.max_conns_per_host", 20)
	viper.SetDefault("http.client.idle_conn_timeout", 60)
	viper.SetDefault("http.client.keep_alive", 30)
	
	// 限流配置
	viper.SetDefault("rate_limit.refill_interval_ms", 200)
	viper.SetDefault("rate_limit.default_capacity", 100)
	viper.SetDefault("rate_limit.default_refill_rate", 10)
	
	// 日志配置
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "text")
	
	// Topic映射配置
	viper.SetDefault("topic_mapping.data_topic_prefix", "data.")
	viper.SetDefault("topic_mapping.status_topic", "tasks.status")
}