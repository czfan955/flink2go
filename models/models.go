package models

// RawMessage Kafka 原始消息结构
type RawMessage struct {
	Minute int     `json:"minute"`
	AdType int     `json:"adType"`
	Brand  string  `json:"brand"`
	Show   int     `json:"show"`
	Click  int     `json:"click"`
	Price  float64 `json:"price"`
}

// AdTypeMapping 广告类型映射
type AdTypeMapping struct {
	AdTypeID   int    `db:"ad_type_id"`
	AdTypeName string `db:"ad_type_name"`
}

// ProcessedMessage 处理后的消息
type ProcessedMessage struct {
	Minute     int
	AdType     int
	AdTypeName string
	Brand      string
	Show       int
	Click      int
	Price      float64
}

// GroupKey 聚合分组键
type GroupKey struct {
	Minute int
	Brand  string
}

// AggValue 聚合值
type AggValue struct {
	AdType     int
	AdTypeName string
	TotalShow  int64
	TotalClick int64
	TotalPrice float64
	Count      int64
}

// AggRecord 聚合记录，用于 Bucket 通道传输
type AggRecord struct {
	Key   GroupKey
	Value AggValue
}

// FlushSignal Flush 信号
type FlushSignal struct{}

// FlushTask Flush 任务
type FlushTask struct {
	Data map[GroupKey]AggValue
}

// ClickHouseRow ClickHouse 写入行
type ClickHouseRow struct {
	Minute     int
	Brand      string
	AdType     int
	AdTypeName string
	TotalShow  int64
	TotalClick int64
	TotalPrice float64
	Count      int64
}
