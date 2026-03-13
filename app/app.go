package app

import (
	"context"
	"log"
	"sync"

	"flink2go/aggregator"
	"flink2go/cache"
	"flink2go/config"
	"flink2go/consumer"
	"flink2go/models"
	"flink2go/processor"
	"flink2go/writer"
)

// App 应用程序
type App struct {
	cfg      *config.Config
	cache    *cache.MetadataCache
	agg      *aggregator.Aggregator
	writer   *writer.Writer
	consumer *consumer.Consumer
	processor *processor.Processor

	msgChan   chan []byte
	taskQueue chan models.FlushTask

	stopFuncs []func()
	mu        sync.Mutex
}

// New 创建应用实例
func New(cfgPath string) (*App, error) {
	// 加载配置
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	log.Println("[App] 配置加载成功")

	app := &App{
		cfg:       cfg,
		stopFuncs: make([]func(), 0),
	}

	return app, nil
}

// Init 初始化应用组件
func (a *App) Init(ctx context.Context) error {
	// 1. 初始化 Redis 元数据缓存
	metadataCache, err := cache.NewMetadataCache(&a.cfg.Redis)
	if err != nil {
		return err
	}
	metadataCache.Start()
	a.cache = metadataCache
	a.addStopFunc(metadataCache.Stop)
	log.Println("[App] Redis 元数据缓存初始化成功")

	// 2. 创建消息通道
	a.msgChan = make(chan []byte, a.cfg.Kafka.ChannelSize)

	// 3. 创建 Flush 任务队列
	a.taskQueue = make(chan models.FlushTask, a.cfg.Writer.WorkerCount*2)

	// 4. 初始化聚合器
	a.agg = aggregator.NewAggregator(&a.cfg.Aggregator, a.taskQueue)
	a.addStopFunc(a.agg.Stop)

	// 5. 初始化 ClickHouse 写入器
	chWriter, err := writer.NewWriter(&a.cfg.ClickHouse, a.taskQueue, a.cfg.Writer.WorkerCount, a.cfg.Writer.BatchSize)
	if err != nil {
		return err
	}

	// 检查连接
	if err := chWriter.Ping(ctx); err != nil {
		return err
	}

	// 创建表（如果不存在）
	if err := chWriter.CreateTable(ctx, a.cfg.ClickHouse.Database, a.cfg.ClickHouse.Table); err != nil {
		log.Printf("[App] 创建 ClickHouse 表失败: %v", err)
	}

	chWriter.Start(ctx, a.cfg.ClickHouse.Table)
	a.writer = chWriter
	a.addStopFunc(chWriter.Stop)
	log.Println("[App] ClickHouse 写入器初始化成功")

	// 6. 初始化 Kafka 消费者
	kafkaConsumer := consumer.NewConsumer(&a.cfg.Kafka, a.msgChan)
	kafkaConsumer.Start(ctx)
	a.consumer = kafkaConsumer
	a.addStopFunc(kafkaConsumer.Stop)
	log.Println("[App] Kafka 消费者初始化成功")

	// 7. 初始化消息处理器
	a.processor = processor.NewProcessor(metadataCache)

	return nil
}

// Start 启动消息处理协程
func (a *App) Start(ctx context.Context) {
	go a.processMessages(ctx)
	log.Println("[App] 消息处理协程已启动")
}

// processMessages 消息处理循环
func (a *App) processMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-a.msgChan:
			msg, err := a.processor.Process(data)
			if err != nil {
				continue
			}
			a.agg.Submit(msg)
		}
	}
}

// Stop 停止应用
func (a *App) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 按逆序执行停止函数
	for i := len(a.stopFuncs) - 1; i >= 0; i-- {
		a.stopFuncs[i]()
	}
	log.Println("[App] 应用已停止")
}

// addStopFunc 添加停止函数
func (a *App) addStopFunc(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stopFuncs = append(a.stopFuncs, fn)
}

// GetAggregator 获取聚合器
func (a *App) GetAggregator() *aggregator.Aggregator {
	return a.agg
}

// GetCache 获取缓存
func (a *App) GetCache() *cache.MetadataCache {
	return a.cache
}

// GetConfig 获取配置
func (a *App) GetConfig() *config.Config {
	return a.cfg
}
