package cache

import (
	"database/sql"
	"log"
	"sync/atomic"
	"time"

	"flink2go/config"
	"flink2go/models"

	_ "github.com/go-sql-driver/mysql"
)

// MetadataCache 元数据缓存
type MetadataCache struct {
	db       *sql.DB
	data     atomic.Value // 存储 map[int]string
	interval time.Duration
	stopCh   chan struct{}
}

// NewMetadataCache 创建元数据缓存
func NewMetadataCache(cfg *config.MySQLConfig) (*MetadataCache, error) {
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, err
	}

	// 配置连接池
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	cache := &MetadataCache{
		db:       db,
		interval: cfg.RefreshInterval,
		stopCh:   make(chan struct{}),
	}

	// 初始化空 map
	cache.data.Store(make(map[int]string))

	// 首次加载
	if err := cache.refresh(); err != nil {
		log.Printf("[MetadataCache] 首次加载失败: %v", err)
	}

	return cache, nil
}

// Start 启动定时刷新
func (c *MetadataCache) Start() {
	go c.run()
}

// Stop 停止刷新
func (c *MetadataCache) Stop() {
	close(c.stopCh)
	c.db.Close()
}

// run 定时刷新协程
func (c *MetadataCache) run() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if err := c.refresh(); err != nil {
				log.Printf("[MetadataCache] 刷新失败: %v", err)
			}
		}
	}
}

// refresh 刷新缓存
func (c *MetadataCache) refresh() error {
	rows, err := c.db.Query("SELECT ad_type_id, ad_type_name FROM ad_type_mapping")
	if err != nil {
		return err
	}
	defer rows.Close()

	newMap := make(map[int]string)
	for rows.Next() {
		var mapping models.AdTypeMapping
		if err := rows.Scan(&mapping.AdTypeID, &mapping.AdTypeName); err != nil {
			return err
		}
		newMap[mapping.AdTypeID] = mapping.AdTypeName
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// 原子替换
	c.data.Store(newMap)
	log.Printf("[MetadataCache] 刷新完成, 共 %d 条映射", len(newMap))

	return nil
}

// Get 获取映射值
func (c *MetadataCache) Get(adType int) (string, bool) {
	data := c.data.Load().(map[int]string)
	name, ok := data[adType]
	return name, ok
}

// GetAll 获取所有映射
func (c *MetadataCache) GetAll() map[int]string {
	return c.data.Load().(map[int]string)
}
