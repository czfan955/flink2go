package aggregator

import (
	"hash/fnv"
	"log"
	"sync"

	"flink2go/config"
	"flink2go/models"
)

// Bucket 聚合桶
type Bucket struct {
	id        int
	data      map[models.GroupKey]models.AggValue
	ch        chan interface{} // 接收 AggRecord 或 FlushSignal
	taskQueue chan models.FlushTask
	mu        sync.Mutex
}

// NewBucket 创建聚合桶
func NewBucket(id int, channelSize int, taskQueue chan models.FlushTask) *Bucket {
	return &Bucket{
		id:        id,
		data:      make(map[models.GroupKey]models.AggValue),
		ch:        make(chan interface{}, channelSize),
		taskQueue: taskQueue,
	}
}

// Start 启动桶处理协程
func (b *Bucket) Start() {
	go b.run()
}

// Channel 获取桶的通道
func (b *Bucket) Channel() chan interface{} {
	return b.ch
}

// run 桶处理循环
func (b *Bucket) run() {
	for msg := range b.ch {
		switch v := msg.(type) {
		case models.AggRecord:
			b.aggregate(v)
		case models.FlushSignal:
			b.flush()
		}
	}
}

// aggregate 聚合数据
func (b *Bucket) aggregate(record models.AggRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.data[record.Key]; ok {
		existing.TotalShow += record.Value.TotalShow
		existing.TotalClick += record.Value.TotalClick
		existing.TotalPrice += record.Value.TotalPrice
		existing.Count += record.Value.Count
		b.data[record.Key] = existing
	} else {
		b.data[record.Key] = record.Value
	}
}

// flush 刷新数据
func (b *Bucket) flush() {
	b.mu.Lock()
	if len(b.data) == 0 {
		b.mu.Unlock()
		return
	}

	// 交换 Map
	oldData := b.data
	b.data = make(map[models.GroupKey]models.AggValue)
	b.mu.Unlock()

	// 发送到写入队列
	select {
	case b.taskQueue <- models.FlushTask{Data: oldData}:
		log.Printf("[Bucket-%d] Flush %d 条记录到写入队列", b.id, len(oldData))
	default:
		log.Printf("[Bucket-%d] 写入队列已满, 丢弃 %d 条记录", b.id, len(oldData))
	}
}

// Aggregator 聚合器
type Aggregator struct {
	buckets    []*Bucket
	bucketNum  int
	taskQueue  chan models.FlushTask
}

// NewAggregator 创建聚合器
func NewAggregator(cfg *config.AggregatorConfig, taskQueue chan models.FlushTask) *Aggregator {
	agg := &Aggregator{
		buckets:   make([]*Bucket, cfg.BucketCount),
		bucketNum: cfg.BucketCount,
		taskQueue: taskQueue,
	}

	for i := 0; i < cfg.BucketCount; i++ {
		agg.buckets[i] = NewBucket(i, cfg.ChannelSize, taskQueue)
		agg.buckets[i].Start()
	}

	log.Printf("[Aggregator] 创建 %d 个聚合桶", cfg.BucketCount)
	return agg
}

// Submit 提交消息进行聚合
func (a *Aggregator) Submit(msg *models.ProcessedMessage) {
	key := models.GroupKey{
		Minute: msg.Minute,
		Brand:  msg.Brand,
	}

	value := models.AggValue{
		AdType:     msg.AdType,
		AdTypeName: msg.AdTypeName,
		TotalShow:  int64(msg.Show),
		TotalClick: int64(msg.Click),
		TotalPrice: msg.Price,
		Count:      1,
	}

	record := models.AggRecord{
		Key:   key,
		Value: value,
	}

	// 计算 shard index
	shardIndex := a.getShardIndex(key)

	// 发送到对应桶
	select {
	case a.buckets[shardIndex].Channel() <- record:
	default:
		log.Printf("[Aggregator] Bucket-%d 通道已满, 丢弃消息", shardIndex)
	}
}

// Flush 触发所有桶刷新
func (a *Aggregator) Flush() {
	log.Println("[Aggregator] 触发 Flush")
	for _, bucket := range a.buckets {
		select {
		case bucket.Channel() <- models.FlushSignal{}:
		default:
			log.Printf("[Aggregator] Bucket-%d 通道已满, Flush 信号丢弃", bucket.id)
		}
	}
}

// Stop 停止聚合器
func (a *Aggregator) Stop() {
	for _, bucket := range a.buckets {
		close(bucket.Channel())
	}
	log.Println("[Aggregator] 已停止")
}

// getShardIndex 计算分片索引
func (a *Aggregator) getShardIndex(key models.GroupKey) int {
	h := fnv.New32a()
	h.Write([]byte(string(rune(key.Minute)) + "|" + key.Brand))
	return int(h.Sum32()) % a.bucketNum
}
