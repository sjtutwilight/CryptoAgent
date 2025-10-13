package config

import (
	"fmt"
	"os"
	"time"

	yaml "gopkg.in/yaml.v3"
)

type RoleConfig struct {
    RoleID          string `yaml:"role_id"`
    Emitter         string `yaml:"emitter"`          // "polling" | "single"
    PollingInterval int    `yaml:"polling_interval"` // seconds when emitter==polling

    EmitterConfig map[string]any `yaml:"emitter_config"`

    Caller       string         `yaml:"caller"`        // "sdk_call" | "native_call"
    CallerClass  string         `yaml:"caller_class"`  // e.g., "LocalGetBlock" (for sdk_call)
    CallerConfig map[string]any `yaml:"caller_config"` // caller级别配置(如protocol, url等)
    CallerParams map[string]any `yaml:"caller_params"` // caller参数(如订阅参数、重连配置等)

	Queue struct {
		Size int `yaml:"size"`
	} `yaml:"queue"`

	Handlers []HandlerConfig `yaml:"handlers"`
	Sink     SinkConfig      `yaml:"sink"`
}

type Config struct {
	Roles []RoleConfig `yaml:"roles"`
}

type HandlerConfig struct {
	Type string         `yaml:"type"`
	With map[string]any `yaml:"with"`
}

type SinkConfig struct {
	Type string         `yaml:"type"`
	With map[string]any `yaml:"with"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	for i := range cfg.Roles {
		if err := cfg.Roles[i].validate(); err != nil {
			return nil, fmt.Errorf("role[%d] %w", i, err)
		}
	}
	return &cfg, nil
}

func (r *RoleConfig) validate() error {
	if r.RoleID == "" {
		return fmt.Errorf("invalid role: missing role_id")
	}
    // 支持多种 emitter
    switch r.Emitter {
    case "polling":
        if r.PollingInterval <= 0 {
            r.PollingInterval = 1
        }
    case "single":
        // single emitter 不需要 polling_interval
    case "kafka_command":
        if r.EmitterConfig == nil {
            return fmt.Errorf("role %s: kafka_command emitter requires emitter_config", r.RoleID)
        }
    default:
        return fmt.Errorf("role %s: unsupported emitter %q", r.RoleID, r.Emitter)
    }

	// 支持sdk_call、balance_snapshot、native_call
    switch r.Caller {
    case "sdk_call", "balance_snapshot":
        if r.CallerClass == "" {
            return fmt.Errorf("role %s: missing caller_class", r.RoleID)
        }
	case "native_call":
		if r.CallerConfig == nil {
			return fmt.Errorf("role %s: native_call requires caller_config", r.RoleID)
		}
	default:
		return fmt.Errorf("role %s: unsupported caller %q", r.RoleID, r.Caller)
	}

	if r.Queue.Size <= 0 {
		r.Queue.Size = 1000
	}
	if r.Sink.Type == "" {
		r.Sink.Type = "console"
	}
	return nil
}

func (r RoleConfig) PollingDuration() time.Duration {
    return time.Duration(r.PollingInterval) * time.Second
}
