package config

import (
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// Config represents the application configuration
type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Chain    ChainConfig    `yaml:"chain"`
	Services ServicesConfig `yaml:"services"`
}

// PostgresConfig represents PostgreSQL configuration
type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"sslmode"`
}

// KafkaConfig represents Kafka configuration
type KafkaConfig struct {
	Brokers []string    `yaml:"brokers"`
	Topics  TopicConfig `yaml:"topics"`
}

// TopicConfig represents Kafka topic configuration
type TopicConfig struct {
	ChainTransactions string `yaml:"chain_transactions"`
}

type ChainConfig struct {
	RPCURL  string `yaml:"rpc_url"`
	ChainID int64  `yaml:"chain_id"`
}

type ServicesConfig struct {
	Listener    ServiceConfig `yaml:"listener"`
	Strategy    ServiceConfig `yaml:"strategy"`
	DataManager ServiceConfig `yaml:"datamanager"`
}

type ServiceConfig struct {
	Port int `yaml:"port"`
}

// DeploymentConfig represents the structure of deployment.json
type DeploymentConfig struct {
	Factory      string        `json:"factory"`
	Router       string        `json:"router"`
	InitCodeHash string        `json:"initCodeHash"`
	Tokens       []TokenConfig `json:"tokens"`
	Pairs        []PairConfig  `json:"pairs"`
}

type TokenConfig struct {
	Address  string `json:"address"`
	Symbol   string `json:"symbol"`
	Decimals string `json:"decimals"`
}

type PairConfig struct {
	Token0  string `json:"token0"`
	Token1  string `json:"token1"`
	Address string `json:"address"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing config file: %w", err)
	}

	return &config, nil
}

// LoadDeploymentConfig loads deployment configuration from the specified JSON file
func LoadDeploymentConfig(filename string) (*DeploymentConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading deployment config file: %w", err)
	}

	var config DeploymentConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing deployment config file: %w", err)
	}

	return &config, nil
}
