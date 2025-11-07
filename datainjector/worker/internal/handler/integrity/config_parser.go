package integrity

import (
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// ParseConfig 从配置字典解析 Integrity 配置
func ParseConfig(cfg map[string]any) (Config, error) {
	var out Config
	out.Profile = util.GetString(cfg, "profile", util.GetString(cfg, "strategy", "generic"))
	out.Keys.SeqField = util.GetString(cfg, "sequence_field", "")
	if out.Keys.SeqField == "" {
		out.Keys.SeqField = util.GetString(cfg, "seq_field", "")
	}
	out.Keys.RangeStartField = util.GetString(cfg, "range_start_field", "")
	out.Keys.StreamKeyField = util.GetString(cfg, "stream_key_field", "")
	out.Keys.MessageIDFields = util.GetStringSlice(cfg, "message_id_fields")

	out.Sequence.EagerGap = uint64(util.GetInt(cfg, "eager_gap", 3))
	out.Sequence.MaxRange = uint64(util.GetInt(cfg, "max_range", 20))
	out.Sequence.MaxDelay = util.ToDuration(util.GetInt(cfg, "max_delay_ms", 800), time.Millisecond)
	out.Sequence.HardTimeout = util.ToDuration(util.GetInt(cfg, "hard_timeout_ms", 3000), time.Millisecond)
	out.Sequence.MaxGap = uint64(util.GetInt(cfg, "max_gap", 8))

	out.Buffer.TTL = util.ToDuration(util.GetInt(cfg, "bucket_ttl_ms", 3000), time.Millisecond)
	out.Buffer.MaxBuckets = util.GetInt(cfg, "max_buckets", 2000)
	out.Buffer.SweepEvery = util.ToDuration(util.GetInt(cfg, "sweep_interval_ms", 200), time.Millisecond)

	out.Dedupe.Enabled = util.GetBool(cfg, "dedupe_enabled", false)
	out.Dedupe.TTL = util.ToDuration(util.GetInt(cfg, "dedupe_ttl_ms", 5000), time.Millisecond)

	out.Gate.Mode = util.GetString(cfg, "gate_mode", "")
	out.Gate.FinalityBlocks = util.GetInt(cfg, "finality_blocks", 12)

	out.Backfill.Options = parseBackfillOptions(cfg)
	out.Backfill.Cooldown = util.ToDuration(util.GetInt(cfg, "backfill_cooldown_ms", 3000), time.Millisecond)

	out.Normalise()
	if err := out.validate(); err != nil {
		return Config{}, err
	}
	return out, nil
}

func parseBackfillOptions(cfg map[string]any) []types.BackfillOption {
	raw, ok := cfg["backfill"].(map[string]any)
	if !ok {
		return nil
	}
	var options []types.BackfillOption

	if wsCfg, ok := raw["ws"].(map[string]any); ok && util.GetBool(wsCfg, "enabled", false) {
		params := map[string]any{}
		if util.GetBool(wsCfg, "include_full_tx", false) {
			params["include_full_tx"] = true
		}
		if endpoint, ok := wsCfg["endpoint"].(string); ok {
			params["endpoint"] = endpoint
		}
		if query, ok := wsCfg["query"].(map[string]any); ok {
			params["query"] = query
		}
		options = append(options, types.BackfillOption{
			Transport: types.BackfillTransportWebSocket,
			RPCMethod: util.GetString(wsCfg, "rpc_method", "eth_getBlockByNumber"),
			Params:    params,
		})
	}

	if httpCfg, ok := raw["http"].(map[string]any); ok && util.GetBool(httpCfg, "enabled", false) {
		params := map[string]any{}
		if util.GetBool(httpCfg, "include_full_tx", false) {
			params["include_full_tx"] = true
		}
		if endpoint, ok := httpCfg["endpoint"].(string); ok {
			params["endpoint"] = endpoint
		}
		if method, ok := httpCfg["method"].(string); ok {
			params["method"] = method
		}
		if query, ok := httpCfg["query"].(map[string]any); ok {
			params["query"] = query
		}
		if headers, ok := httpCfg["headers"].(map[string]any); ok {
			params["headers"] = headers
		}
		options = append(options, types.BackfillOption{
			Transport: types.BackfillTransportHTTP,
			RPCMethod: util.GetString(httpCfg, "rpc_method", "eth_getBlockByNumber"),
			Params:    params,
		})
	}

	return options
}
