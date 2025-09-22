package resolvers

// TimeWindow 表示时间窗口枚举
type TimeWindow int

const (
	// TimeWindowTwentySeconds 表示20秒时间窗口
	TimeWindowTwentySeconds TimeWindow = iota
	// TimeWindowOneMinute 表示1分钟时间窗口
	TimeWindowOneMinute
	// TimeWindowFiveMinutes 表示5分钟时间窗口
	TimeWindowFiveMinutes
	// TimeWindowOneHour 表示1小时时间窗口
	TimeWindowOneHour
)
