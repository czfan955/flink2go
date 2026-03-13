package consumer

import (
	"context"
	"log"
	"sync"
	"time"

	"flink2go/config"

	"github.com/segmentio/kafka-go"
)

// Consumer Kafka 消费者
type Consumer struct {
	reader   *kafka.Reader
	msgChan  chan []byte
	wg       sync.WaitGroup
	stopCh   chan struct{}
	workers  int
}

// NewConsumer 创建 Kafka 消费者
func NewConsumer(cfg *config.KafkaConfig, msgChan chan []byte) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 10e3,  // 10KB
		MaxBytes: 10e6,  // 10MB
		MaxWait:  time.Second * 3, // 最多等待3秒
	})

	return &Consumer{
		reader:  reader,
		msgChan: msgChan,
		stopCh:  make(chan struct{}),
		workers: cfg.ConsumerCount,
	}
}

// Start 启动消费者
func (c *Consumer) Start(ctx context.Context) {
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.consume(ctx, i)
	}
	log.Printf("[Consumer] 启动 %d 个消费协程", c.workers)
}

// Stop 停止消费者
func (c *Consumer) Stop() {
	close(c.stopCh)
	c.wg.Wait()
	c.reader.Close()
	log.Println("[Consumer] 已停止")
}

// consume 消费协程
func (c *Consumer) consume(ctx context.Context, workerID int) {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		default:
			msg, err := c.reader.ReadMessage(ctx)
			log.Printf("[Consumer-%d] 读取消息: %s", workerID, string(msg.Value))
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[Consumer-%d] 读取消息失败: %v", workerID, err)
				continue
			}

			// 投递到共享 channel
			select {
			case c.msgChan <- msg.Value:
			case <-c.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}
