package tracing

import (
	"math/rand"
	"time"
)

type Config struct {
	Enabled           bool     `yaml:"enabled" json:"enabled"`
	ServiceName       string   `yaml:"service_name" json:"service_name"`
	SampleRatio       float64  `yaml:"sample_ratio" json:"sample_ratio"`
	ForceSampleRunID  bool     `yaml:"force_sample_run_id" json:"force_sample_run_id"`
	ForceSampleRoleID []string `yaml:"force_sample_role_ids" json:"force_sample_role_ids"`
}

var cfg = Config{
	Enabled:     true,
	SampleRatio: 0.01,
}

func Init(c Config) {
	if c.SampleRatio <= 0 || c.SampleRatio > 1 {
		c.SampleRatio = cfg.SampleRatio
	}
	if !c.Enabled {
		cfg.Enabled = false
	} else {
		cfg.Enabled = true
	}
	if c.ServiceName != "" {
		cfg.ServiceName = c.ServiceName
	}
	cfg.SampleRatio = c.SampleRatio
	cfg.ForceSampleRunID = c.ForceSampleRunID
	cfg.ForceSampleRoleID = c.ForceSampleRoleID
	rand.Seed(time.Now().UnixNano())
}

func ShouldSample(runID string, roleID string) bool {
	if !cfg.Enabled {
		return false
	}
	if cfg.ForceSampleRunID && runID != "" {
		return true
	}
	for _, id := range cfg.ForceSampleRoleID {
		if id == roleID {
			return true
		}
	}
	if cfg.SampleRatio <= 0 {
		return false
	}
	return rand.Float64() < cfg.SampleRatio
}
