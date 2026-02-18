package integrity

import (
	"fmt"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// Event 表示进入正确性模块的统一事件。
type Event struct {
	Seq        uint64         // 主序列号，控制核心顺序
	RangeStart uint64         // 范围起点，支持范围覆盖判断
	HasRange   bool           // 是否携带范围信息
	StreamKey  string         // 流身份，用于分片或最终幂等
	MessageID  string         // 业务自定义幂等键
	Arrival    time.Time      // Event 被接收的本地时间
	Message    *types.Message // 原始消息引用
}

// Config 聚合完整、顺序、幂等三个维度的配置。
type Config struct {
	Profile string // 策略档位（generic/binance_depth/chain_blocks 等）

	Keys struct {
		SeqField        string   // 序列号字段名
		RangeStartField string   // 范围起点字段名
		StreamKeyField  string   // 流标识字段名
		MessageIDFields []string // 幂等字段集合
	}

	Sequence struct {
		EagerGap    uint64        // 超过该 gap 立即补数
		MaxRange    uint64        // 补数请求最大跨度
		MaxDelay    time.Duration // 软超时，触发补数
		HardTimeout time.Duration // 硬超时，允许跳跃
		MaxGap      uint64        // 实时容忍的最大 gap
	}

	Buffer struct {
		TTL        time.Duration // 乱序缓存保存时间
		MaxBuckets int           // 乱序缓存上限
		SweepEvery time.Duration // 清理周期
	}

	Dedupe struct {
		Enabled bool          // 是否启用幂等过滤
		TTL     time.Duration // 幂等键保留时长
	}

	Gate struct {
		Mode           string // 放行模式：none/snapshot_hold/finality
		FinalityBlocks int    // finality 模式等待的块数
	}

	Backfill struct {
		Cooldown                time.Duration          // 相同范围补数冷却时间
		Options                 []types.BackfillOption // 可用补数通道
		SnapshotBased           bool                   // 是否基于快照补数（binance orderbook）
		OrderbookMode           string                 // 订单簿模式：snapshot_gate/snapshot_sidechannel
		ResultDrivenEnabled     bool                   // 是否启用结果驱动闭环（feature flag）
		MaxFailures             int                    // 连续失败阈值，超过进入 exhausted 冷静期
		ExhaustCooldown         time.Duration          // exhausted 后冷静期
		RetryBackoff            time.Duration          // attempt 之间的退避
		BackpressureGapCooldown time.Duration          // 背压状态下 gap 触发补数的限频窗口
		EnqueueTimeout          time.Duration          // backfill 指令入队超时
		PersistentCompensation  bool                   // 是否启用持久化补偿
		CompensationFile        string                 // 持久化补偿文件路径
		ReplayInterval          time.Duration          // 持久化补偿重放周期
		CompensationMaxPending  int                    // 持久化补偿最大积压条数
	}
}

// Normalise 根据 profile 填充默认值。
func (c *Config) Normalise() {
	// 默认档位。
	if c.Sequence.MaxDelay == 0 {
		c.Sequence.MaxDelay = 800 * time.Millisecond
	}
	if c.Sequence.HardTimeout == 0 {
		c.Sequence.HardTimeout = 3 * time.Second
	}
	if c.Sequence.EagerGap == 0 {
		c.Sequence.EagerGap = 3
	}
	if c.Sequence.MaxRange == 0 {
		c.Sequence.MaxRange = 20
	}
	if c.Sequence.MaxGap == 0 {
		c.Sequence.MaxGap = 8
	}
	if c.Buffer.TTL == 0 {
		c.Buffer.TTL = 3 * time.Second
	}
	if c.Buffer.SweepEvery == 0 {
		c.Buffer.SweepEvery = 200 * time.Millisecond
	}
	if c.Buffer.MaxBuckets == 0 {
		c.Buffer.MaxBuckets = 2000
	}
	if c.Backfill.Cooldown == 0 {
		c.Backfill.Cooldown = 3 * time.Second
	}
	if c.Backfill.MaxFailures <= 0 {
		c.Backfill.MaxFailures = 3
	}
	if c.Backfill.ExhaustCooldown <= 0 {
		c.Backfill.ExhaustCooldown = 30 * time.Second
	}
	if c.Backfill.RetryBackoff < 0 {
		c.Backfill.RetryBackoff = 0
	}
	if c.Backfill.BackpressureGapCooldown <= 0 {
		c.Backfill.BackpressureGapCooldown = 2 * time.Second
	}
	if c.Backfill.EnqueueTimeout <= 0 {
		c.Backfill.EnqueueTimeout = 200 * time.Millisecond
	}
	if c.Backfill.ReplayInterval <= 0 {
		c.Backfill.ReplayInterval = 2 * time.Second
	}
	if c.Backfill.CompensationMaxPending <= 0 {
		c.Backfill.CompensationMaxPending = 2000
	}
	if strings.TrimSpace(c.Backfill.CompensationFile) == "" {
		c.Backfill.CompensationFile = "runtime/data/backfill_compensation.json"
	}

	switch strings.ToLower(c.Profile) {
	case "", "generic":
		// 已采用默认值。
	case "binance_depth":
		if c.Keys.RangeStartField == "" {
			c.Keys.RangeStartField = "first_update_id"
		}
		// Binance 订单簿使用快照补数，不是范围补数
		c.Backfill.SnapshotBased = true
		mode := strings.ToLower(strings.TrimSpace(c.Backfill.OrderbookMode))
		if mode == "" {
			mode = "snapshot_gate"
		}
		c.Backfill.OrderbookMode = mode
		if mode == "snapshot_sidechannel" {
			// sidechannel 模式下必须关闭 snapshot gate，diff 主流不能被阻塞。
			c.Gate.Mode = "none"
		} else if c.Gate.Mode == "" {
			c.Gate.Mode = "snapshot_hold"
		}
	case "chain_blocks":
		if c.Gate.Mode == "" {
			c.Gate.Mode = "finality"
		}
		if c.Gate.FinalityBlocks == 0 {
			c.Gate.FinalityBlocks = 12
		}
	default:
		// 未知 profile 不做特殊处理。
	}
}

func (c Config) SnapshotSideChannelEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(c.Backfill.OrderbookMode), "snapshot_sidechannel")
}

func (c *Config) validate() error {
	if c.Keys.SeqField == "" {
		return fmt.Errorf("integrity: seq_field 必填")
	}
	return nil
}

// toUint64 使用 util 包的实现
var toUint64 = util.ToUint64
