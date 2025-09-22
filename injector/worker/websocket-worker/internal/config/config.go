package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka      KafkaConfig      `yaml:"kafka"`
	Websocket  WebsocketConfig  `yaml:"websocket"`
	Connection ConnectionConfig `yaml:"connection"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topics  struct {
		Binance   string `yaml:"binance"`
		Quicknode string `yaml:"quicknode"`
	} `yaml:"topics"`
}

type WebsocketConfig struct {
	Binance   BinanceConfig   `yaml:"binance"`
	Quicknode QuicknodeConfig `yaml:"quicknode"`
}

type BinanceConfig struct {
	Enable    bool     `yaml:"enable"`
	URL       string   `yaml:"url"`
	ApiKey    string   `yaml:"apiKey"`
	SecretKey string   `yaml:"secretKey"`
	Symbols   []string `yaml:"symbols"`
	Interval  string   `yaml:"interval"`
}

type QuicknodeConfig struct {
	Enable        bool     `yaml:"enable"`
	URL           string   `yaml:"url"`
	ApiKey        string   `yaml:"apiKey"`
	Subscriptions []string `yaml:"subscriptions"`
}

type ConnectionConfig struct {
	Heartbeat struct {
		Interval int `yaml:"interval"`
		Timeout  int `yaml:"timeout"`
	} `yaml:"heartbeat"`
	Reconnect struct {
		MaxRetries  int `yaml:"maxRetries"`
		BackoffBase int `yaml:"backoffBase"`
		BackoffMax  int `yaml:"backoffMax"`
	} `yaml:"reconnect"`
}

func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

