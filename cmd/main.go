package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flink2go/api"
	"flink2go/aggregator"
	"flink2go/cache"
	"flink2go/config"
	"flink2go/consumer"
	"flink2go/models"
	"flink2go/processor"
	"flink2go/writer"

	"github.com/robfig/cron/v3"

	// MySQL 驱动（已注释）
	// _ "github.com/go-sql-driver/mysql"
)

func main() {
	// 加载配置
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	log.Println("配置加载成功")

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建 cron 调度器
	cronScheduler := cron.New(cron.WithSeconds())

	// 1. 初始化 Redis 元数据缓存
	metadataCache, err := cache.NewMetadataCache(&cfg.Redis)
	if err != nil {
		log.Fatalf("初始化元数据缓存失败: %v", err)
	}
	metadataCache.Start()
	defer metadataCache.Stop()
	log.Println("Redis 元数据缓存初始化成功")

	// MySQL 元数据缓存（已注释）
	// metadataCache, err := cache.NewMetadataCache(&cfg.MySQL)
	// if err != nil {
	// 	log.Fatalf("初始化元数据缓存失败: %v", err)
	// }
	// metadataCache.Start()
	// defer metadataCache.Stop()
	// log.Println("MySQL 元数据缓存初始化成功")

	// 2. 创建消息通道
	msgChan := make(chan []byte, cfg.Kafka.ChannelSize)

	// 3. 创建 Flush 任务队列
	taskQueue := make(chan models.FlushTask, cfg.Writer.WorkerCount*2)

	// 4. 初始化聚合器
	agg := aggregator.NewAggregator(&cfg.Aggregator, taskQueue)
	defer agg.Stop()

	// 5. 初始化 ClickHouse 写入器
	chWriter, err := writer.NewWriter(&cfg.ClickHouse, taskQueue, cfg.Writer.WorkerCount, cfg.Writer.BatchSize)
	if err != nil {
		log.Fatalf("初始化 ClickHouse 写入器失败: %v", err)
	}

	// 检查连接
	if err := chWriter.Ping(ctx); err != nil {
		log.Fatalf("连接 ClickHouse 失败: %v", err)
	}

	// 创建表（如果不存在）
	if err := chWriter.CreateTable(ctx, cfg.ClickHouse.Database, cfg.ClickHouse.Table); err != nil {
		log.Printf("创建 ClickHouse 表失败: %v", err)
	}

	chWriter.Start(ctx, cfg.ClickHouse.Table)
	defer chWriter.Stop()
	log.Println("ClickHouse 写入器初始化成功")

	// 6. 初始化 Kafka 消费者
	kafkaConsumer := consumer.NewConsumer(&cfg.Kafka, msgChan)
	kafkaConsumer.Start(ctx)
	defer kafkaConsumer.Stop()
	log.Println("Kafka 消费者初始化成功")

	// 7. 初始化消息处理器
	msgProcessor := processor.NewProcessor(metadataCache)

	// 8. 启动消息处理协程
	go processMessages(ctx, msgChan, msgProcessor, agg)

	// 9. 使用 cron 启动定时 Flush 任务
	_, err = cronScheduler.AddFunc(cfg.Writer.FlushCron, func() {
		agg.Flush()
	})
	if err != nil {
		log.Fatalf("添加 Flush 定时任务失败: %v", err)
	}
	cronScheduler.Start()
	log.Printf("Flush 定时任务已启动，cron: %s", cfg.Writer.FlushCron)

	// runFlushTimer 协程方式（已注释，使用 cron 替代）
	// go runFlushTimer(ctx, agg, cfg.Writer.FlushInterval)

	// 10. 启动 Web 服务
	handler := api.NewHandler(cfg, metadataCache, agg)
	router := api.SetupRouter(handler)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		log.Println("Web 服务启动在 :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Web 服务启动失败: %v", err)
		}
	}()

	// 11. 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("收到退出信号，正在关闭服务...")

	// 停止 cron 调度器
	cronScheduler.Stop()

	// 优雅关闭 Web 服务
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)

	cancel()
	time.Sleep(time.Second * 2) // 等待处理完成
	log.Println("服务已关闭")
}

// processMessages 消息处理循环
func processMessages(ctx context.Context, msgChan <-chan []byte, proc *processor.Processor, agg *aggregator.Aggregator) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-msgChan:
			msg, err := proc.Process(data)
			if err != nil {
				continue
			}
			agg.Submit(msg)
		}
	}
}

// runFlushTimer 定时 Flush（已注释，使用 cron 替代）
// func runFlushTimer(ctx context.Context, agg *aggregator.Aggregator, interval time.Duration) {
// 	ticker := time.NewTicker(interval)
// 	defer ticker.Stop()
//
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case <-ticker.C:
// 			agg.Flush()
// 		}
// 	}
// }
