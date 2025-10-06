package types

// DataSourceMetadata 数据源元数据（由控制面下发）
type DataSourceMetadata struct {
	ID          string                 `json:"id"`           // 数据源ID
	Name        string                 `json:"name"`         // 数据源名称
	Type        string                 `json:"type"`         // 类型（ethereum/binance/...）
	Protocol    ProtocolType           `json:"protocol"`     // 协议
	Endpoint    EndpointConfig         `json:"endpoint"`     // 端点配置
	RateLimit   RateLimitMetadata      `json:"rate_limit"`   // 限流配置
	Subscription SubscriptionMetadata  `json:"subscription"` // 订阅配置（可选）
	CustomConfig map[string]interface{} `json:"custom_config"` // 自定义配置
}

// EndpointConfig 端点配置
type EndpointConfig struct {
	URL     string            `json:"url"`     // 端点URL
	Headers map[string]string `json:"headers"` // 请求头
	Timeout int               `json:"timeout"` // 超时时间（秒）
}

// RateLimitMetadata 限流元数据
type RateLimitMetadata struct {
	RequestsPerMinute int     `json:"requests_per_minute"` // 每分钟请求数
	RequestsPerSecond float64 `json:"requests_per_second"` // 每秒请求数（折算）
	BurstSize         int     `json:"burst_size"`          // 突发大小
}

// SubscriptionMetadata 订阅元数据
type SubscriptionMetadata struct {
	Supported       bool     `json:"supported"`        // 是否支持订阅
	SubscribeMethod string   `json:"subscribe_method"` // 订阅方法名（通用化）
	Topics          []string `json:"topics"`           // 订阅主题
	Params          []interface{} `json:"params"`      // 订阅参数（通用）
}
