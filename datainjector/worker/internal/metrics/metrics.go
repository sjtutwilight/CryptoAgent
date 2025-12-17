// Package metrics 提供Worker的Prometheus指标暴露能力
// 核心指标包括：消息处理量、处理延迟、队列深度、错误计数、WebSocket连接状态等
package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	once     sync.Once
	registry *prometheus.Registry

	// ==================== 消息处理指标 ====================

	// MessagesReceived 接收的消息总数（按role_id, datasource_id分组）
	MessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "messages",
			Name:      "received_total",
			Help:      "接收的消息总数",
		},
		[]string{"role_id", "datasource_id"},
	)

	// MessagesProcessed 处理完成的消息总数
	MessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "messages",
			Name:      "processed_total",
			Help:      "处理完成的消息总数",
		},
		[]string{"role_id", "datasource_id", "status"}, // status: success, error, dropped
	)

	// MessagesSent 发送到Kafka的消息总数
	MessagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "messages",
			Name:      "sent_total",
			Help:      "发送到下游(Kafka)的消息总数",
		},
		[]string{"role_id", "topic"},
	)

	// ProcessingLatency 消息处理延迟（从接收到发送）
	ProcessingLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "messages",
			Name:      "processing_latency_seconds",
			Help:      "消息处理延迟（秒）",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"role_id"},
	)

	// ==================== 队列指标 ====================

	// QueueSize 当前队列深度
	QueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "worker",
			Subsystem: "queue",
			Name:      "size",
			Help:      "当前队列中待处理的消息数量",
		},
		[]string{"role_id"},
	)

	// QueueCapacity 队列容量
	QueueCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "worker",
			Subsystem: "queue",
			Name:      "capacity",
			Help:      "队列最大容量",
		},
		[]string{"role_id"},
	)

	// QueueDropped 因队列满而丢弃的消息数
	QueueDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "queue",
			Name:      "dropped_total",
			Help:      "因队列满而丢弃的消息数",
		},
		[]string{"role_id"},
	)

	// ==================== WebSocket连接指标 ====================

	// WebSocketConnections 当前WebSocket连接数
	WebSocketConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "worker",
			Subsystem: "websocket",
			Name:      "connections",
			Help:      "当前活跃的WebSocket连接数",
		},
		[]string{"role_id", "endpoint"},
	)

	// WebSocketReconnects 重连次数
	WebSocketReconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "websocket",
			Name:      "reconnects_total",
			Help:      "WebSocket重连次数",
		},
		[]string{"role_id", "endpoint"},
	)

	// WebSocketErrors 连接错误数
	WebSocketErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "websocket",
			Name:      "errors_total",
			Help:      "WebSocket连接错误数",
		},
		[]string{"role_id", "endpoint", "error_type"},
	)

	// ==================== HTTP请求指标 ====================

	// HTTPRequests HTTP请求总数
	HTTPRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "HTTP请求总数",
		},
		[]string{"role_id", "endpoint", "method", "status_code"},
	)

	// HTTPLatency HTTP请求延迟
	HTTPLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "http",
			Name:      "request_latency_seconds",
			Help:      "HTTP请求延迟（秒）",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"role_id", "endpoint"},
	)

	// ==================== 完整性模块指标 ====================

	// IntegrityGaps 检测到的序列号gap数
	IntegrityGaps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "integrity",
			Name:      "gaps_total",
			Help:      "检测到的序列号gap总数",
		},
		[]string{"role_id", "stream_key"},
	)

	// IntegrityBackfills 触发的补数请求数
	IntegrityBackfills = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "integrity",
			Name:      "backfills_total",
			Help:      "触发的补数请求总数",
		},
		[]string{"role_id", "backfill_type", "status"}, // status: success, failed
	)

	// IntegrityBufferSize 乱序缓冲区大小
	IntegrityBufferSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "worker",
			Subsystem: "integrity",
			Name:      "buffer_size",
			Help:      "乱序缓冲区当前大小",
		},
		[]string{"role_id", "stream_key"},
	)

	// IntegrityDuplicates 去重过滤的消息数
	IntegrityDuplicates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "integrity",
			Name:      "duplicates_total",
			Help:      "去重过滤的消息总数",
		},
		[]string{"role_id", "stream_key"},
	)

	// ==================== Handler链指标 ====================

	// HandlerLatency 各Handler处理延迟
	HandlerLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "handler",
			Name:      "latency_seconds",
			Help:      "Handler处理延迟（秒）",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
		[]string{"role_id", "handler_type"},
	)

	// HandlerErrors Handler处理错误数
	HandlerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "handler",
			Name:      "errors_total",
			Help:      "Handler处理错误总数",
		},
		[]string{"role_id", "handler_type", "error_type"},
	)

	// ==================== Sink指标 ====================

	// SinkWriteLatency Sink写入延迟
	SinkWriteLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "sink",
			Name:      "write_latency_seconds",
			Help:      "Sink写入延迟（秒）",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"role_id", "sink_type"},
	)

	// SinkErrors Sink写入错误数
	SinkErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "sink",
			Name:      "errors_total",
			Help:      "Sink写入错误总数",
		},
		[]string{"role_id", "sink_type", "error_type"},
	)

	// ==================== 限流指标 ====================

	// RateLimitWaits 限流等待次数
	RateLimitWaits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "ratelimit",
			Name:      "waits_total",
			Help:      "因限流而等待的次数",
		},
		[]string{"role_id", "datasource_id"},
	)

	// RateLimitWaitDuration 限流等待时长
	RateLimitWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "ratelimit",
			Name:      "wait_duration_seconds",
			Help:      "限流等待时长（秒）",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30},
		},
		[]string{"role_id", "datasource_id"},
	)

	// ==================== Role状态指标 ====================

	// RoleStatus Role运行状态（1=运行中，0=停止）
	RoleStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "worker",
			Subsystem: "role",
			Name:      "status",
			Help:      "Role运行状态（1=运行中，0=停止）",
		},
		[]string{"role_id", "emitter_type"},
	)

	// RoleStartTime Role启动时间戳
	RoleStartTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "worker",
			Subsystem: "role",
			Name:      "start_time_seconds",
			Help:      "Role启动时间（Unix时间戳）",
		},
		[]string{"role_id"},
	)

	// ==================== 构建信息 ====================

	// BuildInfo 构建信息
	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "worker",
			Name:      "build_info",
			Help:      "Worker构建信息",
		},
		[]string{"version", "go_version", "build_time"},
	)
)

// Init 初始化metrics注册
func Init() {
	once.Do(func() {
		registry = prometheus.NewRegistry()

		// 注册标准Go运行时指标
		registry.MustRegister(prometheus.NewGoCollector())
		registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

		// 注册自定义指标
		registry.MustRegister(
			// 消息处理
			MessagesReceived,
			MessagesProcessed,
			MessagesSent,
			ProcessingLatency,
			// 队列
			QueueSize,
			QueueCapacity,
			QueueDropped,
			// WebSocket
			WebSocketConnections,
			WebSocketReconnects,
			WebSocketErrors,
			// HTTP
			HTTPRequests,
			HTTPLatency,
			// 完整性模块
			IntegrityGaps,
			IntegrityBackfills,
			IntegrityBufferSize,
			IntegrityDuplicates,
			// Handler
			HandlerLatency,
			HandlerErrors,
			// Sink
			SinkWriteLatency,
			SinkErrors,
			// 限流
			RateLimitWaits,
			RateLimitWaitDuration,
			// Role状态
			RoleStatus,
			RoleStartTime,
			// 构建信息
			BuildInfo,
		)
	})
}

// Handler 返回Prometheus HTTP handler
func Handler() http.Handler {
	Init()
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// RecordMessageReceived 记录接收到的消息
func RecordMessageReceived(roleID, datasourceID string) {
	MessagesReceived.WithLabelValues(roleID, datasourceID).Inc()
}

// RecordMessageProcessed 记录处理完成的消息
func RecordMessageProcessed(roleID, datasourceID, status string) {
	MessagesProcessed.WithLabelValues(roleID, datasourceID, status).Inc()
}

// RecordMessageSent 记录发送的消息
func RecordMessageSent(roleID, topic string) {
	MessagesSent.WithLabelValues(roleID, topic).Inc()
}

// RecordProcessingLatency 记录处理延迟
func RecordProcessingLatency(roleID string, duration time.Duration) {
	ProcessingLatency.WithLabelValues(roleID).Observe(duration.Seconds())
}

// SetQueueSize 设置队列大小
func SetQueueSize(roleID string, size int) {
	QueueSize.WithLabelValues(roleID).Set(float64(size))
}

// SetQueueCapacity 设置队列容量
func SetQueueCapacity(roleID string, capacity int) {
	QueueCapacity.WithLabelValues(roleID).Set(float64(capacity))
}

// RecordQueueDropped 记录队列丢弃
func RecordQueueDropped(roleID string) {
	QueueDropped.WithLabelValues(roleID).Inc()
}

// SetWebSocketConnection 设置WebSocket连接状态
func SetWebSocketConnection(roleID, endpoint string, connected bool) {
	val := 0.0
	if connected {
		val = 1.0
	}
	WebSocketConnections.WithLabelValues(roleID, endpoint).Set(val)
}

// RecordWebSocketReconnect 记录WebSocket重连
func RecordWebSocketReconnect(roleID, endpoint string) {
	WebSocketReconnects.WithLabelValues(roleID, endpoint).Inc()
}

// RecordWebSocketError 记录WebSocket错误
func RecordWebSocketError(roleID, endpoint, errorType string) {
	WebSocketErrors.WithLabelValues(roleID, endpoint, errorType).Inc()
}

// RecordHTTPRequest 记录HTTP请求
func RecordHTTPRequest(roleID, endpoint, method string, statusCode int, duration time.Duration) {
	HTTPRequests.WithLabelValues(roleID, endpoint, method, string(rune(statusCode))).Inc()
	HTTPLatency.WithLabelValues(roleID, endpoint).Observe(duration.Seconds())
}

// RecordIntegrityGap 记录序列号gap
func RecordIntegrityGap(roleID, streamKey string) {
	IntegrityGaps.WithLabelValues(roleID, streamKey).Inc()
}

// RecordIntegrityBackfill 记录补数请求
func RecordIntegrityBackfill(roleID, backfillType, status string) {
	IntegrityBackfills.WithLabelValues(roleID, backfillType, status).Inc()
}

// SetIntegrityBufferSize 设置缓冲区大小
func SetIntegrityBufferSize(roleID, streamKey string, size int) {
	IntegrityBufferSize.WithLabelValues(roleID, streamKey).Set(float64(size))
}

// RecordIntegrityDuplicate 记录重复消息
func RecordIntegrityDuplicate(roleID, streamKey string) {
	IntegrityDuplicates.WithLabelValues(roleID, streamKey).Inc()
}

// RecordHandlerLatency 记录Handler延迟
func RecordHandlerLatency(roleID, handlerType string, duration time.Duration) {
	HandlerLatency.WithLabelValues(roleID, handlerType).Observe(duration.Seconds())
}

// RecordHandlerError 记录Handler错误
func RecordHandlerError(roleID, handlerType, errorType string) {
	HandlerErrors.WithLabelValues(roleID, handlerType, errorType).Inc()
}

// RecordSinkWriteLatency 记录Sink写入延迟
func RecordSinkWriteLatency(roleID, sinkType string, duration time.Duration) {
	SinkWriteLatency.WithLabelValues(roleID, sinkType).Observe(duration.Seconds())
}

// RecordSinkError 记录Sink错误
func RecordSinkError(roleID, sinkType, errorType string) {
	SinkErrors.WithLabelValues(roleID, sinkType, errorType).Inc()
}

// RecordRateLimitWait 记录限流等待
func RecordRateLimitWait(roleID, datasourceID string, duration time.Duration) {
	RateLimitWaits.WithLabelValues(roleID, datasourceID).Inc()
	RateLimitWaitDuration.WithLabelValues(roleID, datasourceID).Observe(duration.Seconds())
}

// SetRoleStatus 设置Role状态
func SetRoleStatus(roleID, emitterType string, running bool) {
	val := 0.0
	if running {
		val = 1.0
	}
	RoleStatus.WithLabelValues(roleID, emitterType).Set(val)
}

// SetRoleStartTime 设置Role启动时间
func SetRoleStartTime(roleID string) {
	RoleStartTime.WithLabelValues(roleID).Set(float64(time.Now().Unix()))
}

// SetBuildInfo 设置构建信息
func SetBuildInfo(version, goVersion, buildTime string) {
	BuildInfo.WithLabelValues(version, goVersion, buildTime).Set(1)
}




