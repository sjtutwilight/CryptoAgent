package config

import (
	"fmt"
	"os"
	"time"

	yaml "gopkg.in/yaml.v3"
)

type RoleConfig struct {
	RoleID           string         `yaml:"role_id" json:"role_id"`
	DataSourceID     string         `yaml:"datasource_id" json:"datasource_id"`
	RoleTemplate     string         `yaml:"template" json:"template"`
	PipelineTemplate string         `yaml:"pipeline" json:"pipeline"`
	Domain           string         `yaml:"domain" json:"domain"`
	Emitter          string         `yaml:"emitter" json:"emitter"`                   // "polling" | "single"
	PollingInterval  int            `yaml:"polling_interval" json:"polling_interval"` // seconds when emitter==polling
	EmitterConfig    map[string]any `yaml:"emitter_config" json:"emitter_config"`
	Caller           string         `yaml:"caller" json:"caller"`               // "sdk_call" | "native_call"
	CallerClass      string         `yaml:"caller_class" json:"caller_class"`   // e.g., "LocalGetBlock" (for sdk_call)
	CallerConfig     map[string]any `yaml:"caller_config" json:"caller_config"` // caller级别配置(如protocol, url等)
	CallerParams     map[string]any `yaml:"caller_params" json:"caller_params"` // caller参数(如订阅参数、重连配置等)

	PipelineMode string `yaml:"pipeline_mode" json:"pipeline_mode"` // "queue" | "direct"

	Queue struct {
		Mode string `yaml:"mode" json:"mode"` // "bounded" | "none"
		Size int    `yaml:"size" json:"size"`
	} `yaml:"queue" json:"queue"`

	Handlers []HandlerConfig `yaml:"handlers" json:"handlers"`
	Sink     SinkConfig      `yaml:"sink" json:"sink"`
}

type Config struct {
	StatusReporter StatusReporterConfig `yaml:"status_reporter" json:"status_reporter"`
	Metrics        MetricsConfig        `yaml:"metrics" json:"metrics"`
	API            APIServerConfig      `yaml:"api_server" json:"api_server"`
	Logging        LoggingConfig        `yaml:"logging" json:"logging"`
	Tracing        TracingConfig        `yaml:"tracing" json:"tracing"`
	DataSources    []DataSourceConfig   `yaml:"datasources" json:"datasources"`
	RateLimit      RateLimitProfiles    `yaml:"rate_limit_profiles" json:"rate_limit_profiles"`
	RoleTemplates  []RoleTemplateConfig `yaml:"role_templates" json:"role_templates"`
	Pipelines      []PipelineConfig     `yaml:"pipeline_templates" json:"pipeline_templates"`
	Roles          []RoleConfig         `yaml:"roles" json:"roles"`
}

// MetricsConfig Prometheus metrics配置
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"` // 是否启用metrics暴露，默认true
	Port    int    `yaml:"port" json:"port"`       // metrics HTTP端口，默认9100
	Path    string `yaml:"path" json:"path"`       // metrics路径，默认/metrics
}

type StatusReporterConfig struct {
	Enabled bool     `yaml:"enabled" json:"enabled"`
	Brokers []string `yaml:"brokers" json:"brokers"`
	Topic   string   `yaml:"topic" json:"topic"`
}

type HandlerConfig struct {
	Type string         `yaml:"type" json:"type"`
	With map[string]any `yaml:"with" json:"with"`
}

type SinkConfig struct {
	Type string         `yaml:"type" json:"type"`
	With map[string]any `yaml:"with" json:"with"`
}

type APIServerConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Addr    string `yaml:"addr" json:"addr"`
	Token   string `yaml:"token" json:"token"`
}

type LoggingConfig struct {
	ServiceName string `yaml:"service_name" json:"service_name"`
	Environment string `yaml:"environment" json:"environment"`
}

type TracingConfig struct {
	Enabled           bool     `yaml:"enabled" json:"enabled"`
	ServiceName       string   `yaml:"service_name" json:"service_name"`
	SampleRatio       float64  `yaml:"sample_ratio" json:"sample_ratio"`
	ForceSampleRunID  bool     `yaml:"force_sample_run_id" json:"force_sample_run_id"`
	ForceSampleRoleID []string `yaml:"force_sample_role_ids" json:"force_sample_role_ids"`
}

type DataSourceConfig struct {
	ID               string     `yaml:"id" json:"id"`
	Labels           []string   `yaml:"labels" json:"labels"`
	Protocol         string     `yaml:"protocol" json:"protocol"` // http/websocket/evm_rpc
	Auth             AuthConfig `yaml:"auth" json:"auth"`         // api_key_env/bearer_env/none
	RateLimitProfile string     `yaml:"rate_limit_profile" json:"rate_limit_profile"`
}

type AuthConfig struct {
	Type           string `yaml:"type" json:"type"` // api_key_env/bearer_env/none
	Header         string `yaml:"header" json:"header"`
	APIKeyEnv      string `yaml:"api_key_env" json:"api_key_env"`
	BearerTokenEnv string `yaml:"bearer_token_env" json:"bearer_token_env"`
}

type RateLimitProfile struct {
	Capacity   int     `yaml:"capacity" json:"capacity"`
	RefillRate float64 `yaml:"refill_rate" json:"refill_rate"`
}

type RateLimitProfiles map[string]RateLimitProfile

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	if cfg.StatusReporter.Topic == "" {
		cfg.StatusReporter.Topic = "tasks.status"
	}
	// 设置metrics默认值
	if cfg.Metrics.Port == 0 {
		cfg.Metrics.Port = 9100
	}
	if cfg.Metrics.Path == "" {
		cfg.Metrics.Path = "/metrics"
	}
	if cfg.API.Enabled && cfg.API.Addr == "" {
		cfg.API.Addr = ":8090"
	}
	if cfg.Logging.ServiceName == "" {
		cfg.Logging.ServiceName = "datainjector-worker"
	}
	if cfg.Tracing.ServiceName == "" {
		cfg.Tracing.ServiceName = cfg.Logging.ServiceName
	}
	if cfg.Tracing.SampleRatio <= 0 || cfg.Tracing.SampleRatio > 1 {
		cfg.Tracing.SampleRatio = 0.01
	}
	if !cfg.Tracing.Enabled {
		if cfg.Tracing.ForceSampleRunID || len(cfg.Tracing.ForceSampleRoleID) > 0 {
			cfg.Tracing.Enabled = true
		}
	}
	if err := ApplyRoleTemplates(cfg.Roles, cfg.RoleTemplates); err != nil {
		return nil, err
	}
	if err := ApplyPipelineTemplates(cfg.Roles, cfg.Pipelines); err != nil {
		return nil, err
	}
	if err := ApplyDataSourcesToRoles(cfg.Roles, cfg.DataSources, cfg.RateLimit); err != nil {
		return nil, err
	}
	for i := range cfg.Roles {
		if err := cfg.Roles[i].Validate(); err != nil {
			return nil, fmt.Errorf("role[%d] %w", i, err)
		}
	}
	return &cfg, nil
}

type RoleRegistry struct {
	Roles []RoleConfig `yaml:"roles" json:"roles"`
}

func LoadRoles(path string) ([]RoleConfig, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var reg RoleRegistry
	if err := yaml.Unmarshal(b, &reg); err != nil {
		return nil, err
	}
	return reg.Roles, nil
}

func (r *RoleConfig) Validate() error {
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

	// 支持sdk_call、balance_snapshot、native_call、metadata_kafka
	switch r.Caller {
	case "sdk_call", "balance_snapshot":
		if r.CallerClass == "" {
			return fmt.Errorf("role %s: missing caller_class", r.RoleID)
		}
	case "native_call":
		if r.CallerConfig == nil {
			return fmt.Errorf("role %s: native_call requires caller_config", r.RoleID)
		}
	case "batch_file":
		if r.CallerConfig == nil {
			return fmt.Errorf("role %s: batch_file requires caller_config", r.RoleID)
		}
	case "metadata_kafka", "metadata_postgres", "metadata_clickhouse":
		if r.CallerParams == nil {
			return fmt.Errorf("role %s: %s requires caller_params", r.RoleID, r.Caller)
		}
	default:
		return fmt.Errorf("role %s: unsupported caller %q", r.RoleID, r.Caller)
	}

	if r.Queue.Size <= 0 {
		r.Queue.Size = 1000
	}
	if r.Queue.Mode == "" {
		r.Queue.Mode = "bounded"
	}
	switch r.Queue.Mode {
	case "bounded", "none":
	default:
		return fmt.Errorf("role %s: unsupported queue mode %q", r.RoleID, r.Queue.Mode)
	}

	if r.PipelineMode == "" {
		r.PipelineMode = "queue"
	}
	switch r.PipelineMode {
	case "queue", "direct":
	default:
		return fmt.Errorf("role %s: unsupported pipeline mode %q", r.RoleID, r.PipelineMode)
	}
	if r.Queue.Mode == "none" {
		r.PipelineMode = "direct"
	}
	if r.PipelineMode == "queue" && r.Queue.Mode == "none" {
		return fmt.Errorf("role %s: pipeline queue requires queue.mode bounded", r.RoleID)
	}
	if r.Queue.Mode == "none" {
		r.Queue.Size = 0
	}
	if r.Sink.Type == "" {
		r.Sink.Type = "console"
	}
	return nil
}

func (r RoleConfig) PollingDuration() time.Duration {
	return time.Duration(r.PollingInterval) * time.Second
}

func ApplyDataSourcesToRoles(roles []RoleConfig, dataSources []DataSourceConfig, profiles RateLimitProfiles) error {
	if len(dataSources) == 0 {
		return nil
	}
	dsMap := make(map[string]DataSourceConfig, len(dataSources))
	for _, ds := range dataSources {
		if ds.ID == "" {
			return fmt.Errorf("datasource: missing id")
		}
		if _, ok := dsMap[ds.ID]; ok {
			return fmt.Errorf("datasource: duplicate id %q", ds.ID)
		}
		dsMap[ds.ID] = ds
	}

	for i := range roles {
		role := &roles[i]
		if role.DataSourceID == "" {
			continue
		}
		ds, ok := dsMap[role.DataSourceID]
		if !ok {
			return fmt.Errorf("role %s: datasource %q not found", role.RoleID, role.DataSourceID)
		}
		applyDataSourceToRole(role, ds, profiles)
	}

	return nil
}

type RoleTemplateConfig struct {
	ID              string         `yaml:"id" json:"id"`
	Emitter         string         `yaml:"emitter" json:"emitter"`
	PollingInterval int            `yaml:"polling_interval" json:"polling_interval"`
	EmitterConfig   map[string]any `yaml:"emitter_config" json:"emitter_config"`
	Caller          string         `yaml:"caller" json:"caller"`
	CallerClass     string         `yaml:"caller_class" json:"caller_class"`
	CallerConfig    map[string]any `yaml:"caller_config" json:"caller_config"`
	CallerParams    map[string]any `yaml:"caller_params" json:"caller_params"`
	Queue           struct {
		Mode string `yaml:"mode" json:"mode"`
		Size int    `yaml:"size" json:"size"`
	} `yaml:"queue" json:"queue"`
	PipelineMode string `yaml:"pipeline_mode" json:"pipeline_mode"`
}

type PipelineConfig struct {
	ID       string          `yaml:"id" json:"id"`
	Domain   string          `yaml:"domain" json:"domain"`
	Handlers []HandlerConfig `yaml:"handlers" json:"handlers"`
	Sink     SinkConfig      `yaml:"sink" json:"sink"`
	Queue    struct {
		Mode string `yaml:"mode" json:"mode"`
		Size int    `yaml:"size" json:"size"`
	} `yaml:"queue" json:"queue"`
	PipelineMode string `yaml:"pipeline_mode" json:"pipeline_mode"`
}

func ApplyRoleTemplates(roles []RoleConfig, templates []RoleTemplateConfig) error {
	if len(templates) == 0 {
		return nil
	}
	tplMap := make(map[string]RoleTemplateConfig, len(templates))
	for _, tpl := range templates {
		if tpl.ID == "" {
			return fmt.Errorf("role_template: missing id")
		}
		if _, ok := tplMap[tpl.ID]; ok {
			return fmt.Errorf("role_template: duplicate id %q", tpl.ID)
		}
		tplMap[tpl.ID] = tpl
	}

	for i := range roles {
		role := &roles[i]
		if role.RoleTemplate == "" {
			continue
		}
		tpl, ok := tplMap[role.RoleTemplate]
		if !ok {
			return fmt.Errorf("role %s: template %q not found", role.RoleID, role.RoleTemplate)
		}
		applyRoleTemplate(role, tpl)
	}
	return nil
}

func ApplyPipelineTemplates(roles []RoleConfig, pipelines []PipelineConfig) error {
	if len(pipelines) == 0 {
		return nil
	}
	pipeMap := make(map[string]PipelineConfig, len(pipelines))
	for _, tpl := range pipelines {
		if tpl.ID == "" {
			return fmt.Errorf("pipeline_template: missing id")
		}
		if _, ok := pipeMap[tpl.ID]; ok {
			return fmt.Errorf("pipeline_template: duplicate id %q", tpl.ID)
		}
		pipeMap[tpl.ID] = tpl
	}

	for i := range roles {
		role := &roles[i]
		if role.PipelineTemplate == "" {
			continue
		}
		tpl, ok := pipeMap[role.PipelineTemplate]
		if !ok {
			return fmt.Errorf("role %s: pipeline %q not found", role.RoleID, role.PipelineTemplate)
		}
		applyPipelineTemplate(role, tpl)
	}
	return nil
}

func applyRoleTemplate(role *RoleConfig, tpl RoleTemplateConfig) {
	if role.Emitter == "" {
		role.Emitter = tpl.Emitter
	}
	if role.PollingInterval == 0 && tpl.PollingInterval > 0 {
		role.PollingInterval = tpl.PollingInterval
	}
	if tpl.EmitterConfig != nil {
		role.EmitterConfig = mergeDefaults(role.EmitterConfig, tpl.EmitterConfig)
	}
	if role.Caller == "" {
		role.Caller = tpl.Caller
	}
	if role.CallerClass == "" {
		role.CallerClass = tpl.CallerClass
	}
	if tpl.CallerConfig != nil {
		role.CallerConfig = mergeDefaults(role.CallerConfig, tpl.CallerConfig)
	}
	if tpl.CallerParams != nil {
		role.CallerParams = mergeDefaults(role.CallerParams, tpl.CallerParams)
	}
	if role.Queue.Mode == "" && tpl.Queue.Mode != "" {
		role.Queue.Mode = tpl.Queue.Mode
	}
	if role.Queue.Size == 0 && tpl.Queue.Size > 0 {
		role.Queue.Size = tpl.Queue.Size
	}
	if role.PipelineMode == "" && tpl.PipelineMode != "" {
		role.PipelineMode = tpl.PipelineMode
	}
}

func applyPipelineTemplate(role *RoleConfig, tpl PipelineConfig) {
	if role.Domain == "" && tpl.Domain != "" {
		role.Domain = tpl.Domain
	}
	if len(role.Handlers) == 0 && len(tpl.Handlers) > 0 {
		role.Handlers = append([]HandlerConfig{}, tpl.Handlers...)
	}
	if role.Sink.Type == "" && tpl.Sink.Type != "" {
		role.Sink = tpl.Sink
	}
	if role.Queue.Mode == "" && tpl.Queue.Mode != "" {
		role.Queue.Mode = tpl.Queue.Mode
	}
	if role.Queue.Size == 0 && tpl.Queue.Size > 0 {
		role.Queue.Size = tpl.Queue.Size
	}
	if role.PipelineMode == "" && tpl.PipelineMode != "" {
		role.PipelineMode = tpl.PipelineMode
	}
}

func cloneAnyMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		if nested, ok := v.(map[string]any); ok {
			dst[k] = cloneAnyMap(nested)
		} else {
			dst[k] = v
		}
	}
	return dst
}

func mergeDefaults(dst map[string]any, defaults map[string]any) map[string]any {
	if defaults == nil {
		return dst
	}
	if dst == nil {
		return cloneAnyMap(defaults)
	}
	for key, val := range defaults {
		if existing, ok := dst[key]; ok {
			dstMap, dstOK := existing.(map[string]any)
			defMap, defOK := val.(map[string]any)
			if dstOK && defOK {
				dst[key] = mergeDefaults(dstMap, defMap)
			}
			continue
		}
		if defMap, ok := val.(map[string]any); ok {
			dst[key] = cloneAnyMap(defMap)
		} else {
			dst[key] = val
		}
	}
	return dst
}

type RoleValidationError struct {
	RoleID string   `json:"role_id"`
	Errors []string `json:"errors"`
}

func ValidateRoles(roles []RoleConfig) []RoleValidationError {
	out := make([]RoleValidationError, 0)
	for _, role := range roles {
		errs := validateRoleRequirements(role)
		if len(errs) > 0 {
			out = append(out, RoleValidationError{
				RoleID: role.RoleID,
				Errors: errs,
			})
		}
	}
	return out
}

func validateRoleRequirements(role RoleConfig) []string {
	var errs []string
	if err := role.Validate(); err != nil {
		errs = append(errs, err.Error())
	}

	switch role.Emitter {
	case "kafka_command":
		if !hasBrokers(role.EmitterConfig) {
			errs = append(errs, "emitter_config.brokers required for kafka_command")
		}
		if getString(role.EmitterConfig, "topic") == "" {
			errs = append(errs, "emitter_config.topic required for kafka_command")
		}
	}

	if role.Caller == "native_call" {
		if getString(role.CallerConfig, "protocol") == "" {
			errs = append(errs, "caller_config.protocol required for native_call")
		}
	}
	if role.Caller == "batch_file" {
		if getString(role.CallerConfig, "endpoint") == "" {
			errs = append(errs, "caller_config.endpoint required for batch_file")
		}
		if getString(role.CallerConfig, "path_template") == "" {
			errs = append(errs, "caller_config.path_template required for batch_file")
		}
		if getString(role.CallerConfig, "output_dir") == "" {
			errs = append(errs, "caller_config.output_dir required for batch_file")
		}
	}

	switch role.Sink.Type {
	case "kafka":
		if getString(role.Sink.With, "topic") == "" {
			errs = append(errs, "sink.with.topic required for kafka sink")
		}
	case "file":
		if getString(role.Sink.With, "output_dir") == "" {
			errs = append(errs, "sink.with.output_dir required for file sink")
		}
	}

	for _, h := range role.Handlers {
		if requiresSymbol(h.Type) && getString(h.With, "symbol") == "" {
			errs = append(errs, fmt.Sprintf("handler.%s requires with.symbol", h.Type))
		}
	}

	return errs
}

func requiresSymbol(handlerType string) bool {
	switch handlerType {
	case "orderbook_diff",
		"mark_index_parser",
		"funding_normalizer",
		"oi_normalizer",
		"liquidation_normalizer",
		"binance_aggtrade":
		return true
	default:
		return false
	}
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func hasBrokers(m map[string]any) bool {
	if m == nil {
		return false
	}
	raw, ok := m["brokers"]
	if !ok {
		return false
	}
	switch v := raw.(type) {
	case []string:
		return len(v) > 0
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				return true
			}
		}
	case string:
		return v != ""
	}
	return false
}

func applyDataSourceToRole(role *RoleConfig, ds DataSourceConfig, profiles RateLimitProfiles) {
	if role.CallerConfig == nil {
		role.CallerConfig = map[string]any{}
	}
	if _, ok := role.CallerConfig["datasource_id"]; !ok {
		role.CallerConfig["datasource_id"] = ds.ID
	}

	protocol := normalizeProtocol(ds.Protocol)
	if protocol != "" {
		if _, ok := role.CallerConfig["protocol"]; !ok {
			role.CallerConfig["protocol"] = protocol
		}
	}

	if ds.RateLimitProfile != "" {
		if _, ok := role.CallerConfig["rate_limit"]; !ok {
			if profile, ok := profiles[ds.RateLimitProfile]; ok {
				role.CallerConfig["rate_limit"] = map[string]any{
					"capacity":    profile.Capacity,
					"refill_rate": profile.RefillRate,
				}
			}
		}
	}

	applyAuthToCaller(role, ds)
}

func normalizeProtocol(protocol string) string {
	switch protocol {
	case "http", "websocket":
		return protocol
	case "evm_rpc":
		return "http"
	default:
		return ""
	}
}

func applyAuthToCaller(role *RoleConfig, ds DataSourceConfig) {
	if ds.Auth.Type == "" || ds.Auth.Type == "none" {
		return
	}
	headers, _ := role.CallerConfig["headers"].(map[string]any)
	if headers == nil {
		headers = map[string]any{}
		role.CallerConfig["headers"] = headers
	}

	switch ds.Auth.Type {
	case "api_key_env":
		header := ds.Auth.Header
		if header == "" {
			header = "X-API-Key"
		}
		if ds.Auth.APIKeyEnv != "" {
			if _, ok := headers[header]; !ok {
				headers[header] = fmt.Sprintf("${%s}", ds.Auth.APIKeyEnv)
			}
		}
	case "bearer_env":
		if ds.Auth.BearerTokenEnv != "" {
			if _, ok := headers["Authorization"]; !ok {
				headers["Authorization"] = fmt.Sprintf("Bearer ${%s}", ds.Auth.BearerTokenEnv)
			}
		}
	}
}
