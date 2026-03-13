package writer

import (
	"context"
	"log"
	"sync"
	"time"

	"flink2go/config"
	"flink2go/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Writer ClickHouse 写入器
type Writer struct {
	conn       driver.Conn
	taskQueue  chan models.FlushTask
	workers    int
	batchSize  int
	wg         sync.WaitGroup
	stopCh     chan struct{}
}

// NewWriter 创建写入器
func NewWriter(cfg *config.ClickHouseConfig, taskQueue chan models.FlushTask, workerCount int, batchSize int) (*Writer, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})
	if err != nil {
		return nil, err
	}

	return &Writer{
		conn:      conn,
		taskQueue: taskQueue,
		workers:   workerCount,
		batchSize: batchSize,
		stopCh:    make(chan struct{}),
	}, nil
}

// Start 启动写入协程池
func (w *Writer) Start(ctx context.Context, table string) {
	for i := 0; i < w.workers; i++ {
		w.wg.Add(1)
		go w.worker(ctx, i, table)
	}
	log.Printf("[Writer] 启动 %d 个写入协程", w.workers)
}

// Stop 停止写入器
func (w *Writer) Stop() {
	close(w.stopCh)
	w.wg.Wait()
	w.conn.Close()
	log.Println("[Writer] 已停止")
}

// worker 写入协程
func (w *Writer) worker(ctx context.Context, id int, table string) {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case task := <-w.taskQueue:
			if err := w.writeBatch(ctx, table, task.Data); err != nil {
				log.Printf("[Writer-%d] 写入失败: %v", id, err)
			}
		}
	}
}

// writeBatch 批量写入
func (w *Writer) writeBatch(ctx context.Context, table string, data map[models.GroupKey]models.AggValue) error {
	if len(data) == 0 {
		return nil
	}
	log.Printf("[Writer] 准备写入 %d 条记录到 %s", len(data), table)
	// 创建批量插入
	batch, err := w.conn.PrepareBatch(ctx, 
		"INSERT INTO "+table+" (minute, brand, ad_type, ad_type_name, total_show, total_click, total_price, count)")
	if err != nil {
		return err
	}

	for key, value := range data {
		err := batch.Append(
			key.Minute,
			key.Brand,
			value.AdType,
			value.AdTypeName,
			value.TotalShow,
			value.TotalClick,
			value.TotalPrice,
			value.Count,
		)
		if err != nil {
			log.Printf("[Writer] 添加行失败: %v", err)
			continue
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}

	log.Printf("[Writer] 成功写入 %d 条记录到 %s", len(data), table)
	return nil
}

// Ping 检查连接
func (w *Writer) Ping(ctx context.Context) error {
	return w.conn.Ping(ctx)
}

// CreateTable 创建表（如果不存在）
func (w *Writer) CreateTable(ctx context.Context, database, table string) error {
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS ` + database + `.` + table + ` (
			minute Int32,
			brand String,
			ad_type Int32,
			ad_type_name String,
			total_show Int64,
			total_click Int64,
			total_price Float64,
			count Int64,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY (minute, brand)
		PARTITION BY toYYYYMM(toDateTime(minute * 60))
	`

	return w.conn.Exec(ctx, createTableSQL)
}
