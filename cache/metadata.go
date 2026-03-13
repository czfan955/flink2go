package cache

import (
	"context"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"flink2go/config"

	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"

	// MySQL 相关导入（已注释）
	// "database/sql"
	// "flink2go/models"
	// _ "github.com/go-sql-driver/mysql"
)

// MetadataCache 元数据缓存
type MetadataCache struct {
	// MySQL 相关字段（已注释）
	// db       *sql.DB

	rdb      *redis.Client
	data     atomic.Value // 存储 map[int]string
	cronExpr string       // cron 表达式
	hashKey  string       // Redis Hash 键名
	cron     *cron.Cron
}

// NewMetadataCache 创建元数据缓存（Redis 版本）
func NewMetadataCache(cfg *config.RedisConfig) (*MetadataCache, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	cache := &MetadataCache{
		rdb:      rdb,
		cronExpr: cfg.RefreshCron,
		hashKey:  cfg.HashKey,
		cron:     cron.New(cron.WithSeconds()), // 支持秒级定时
	}

	// 初始化空 map
	cache.data.Store(make(map[int]string))

	// 首次加载
	if err := cache.refresh(); err != nil {
		log.Printf("[MetadataCache] 首次加载失败: %v", err)
	}

	return cache, nil
}

// NewMetadataCacheFromMySQL 创建元数据缓存（MySQL 版本，已注释）
// func NewMetadataCacheFromMySQL(cfg *config.MySQLConfig) (*MetadataCache, error) {
// 	db, err := sql.Open("mysql", cfg.DSN)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	// 配置连接池
// 	db.SetMaxOpenConns(10)
// 	db.SetMaxIdleConns(5)
// 	db.SetConnMaxLifetime(time.Hour)
//
// 	cache := &MetadataCache{
// 		db:       db,
// 		interval: cfg.RefreshInterval,
// 		stopCh:   make(chan struct{}),
// 	}
//
// 	// 初始化空 map
// 	cache.data.Store(make(map[int]string))
//
// 	// 首次加载
// 	if err := cache.refresh(); err != nil {
// 		log.Printf("[MetadataCache] 首次加载失败: %v", err)
// 	}
//
// 	return cache, nil
// }

// Start 启动定时刷新
func (c *MetadataCache) Start() {
	// 添加定时任务，使用 cron 表达式
	_, err := c.cron.AddFunc(c.cronExpr, func() {
		if err := c.refresh(); err != nil {
			log.Printf("[MetadataCache] 刷新失败: %v", err)
		}
	})
	if err != nil {
		log.Printf("[MetadataCache] 添加定时任务失败: %v", err)
		return
	}
	c.cron.Start()
	log.Printf("[MetadataCache] 定时刷新已启动，cron: %s", c.cronExpr)
}

// Stop 停止刷新
func (c *MetadataCache) Stop() {
	c.cron.Stop()
	c.rdb.Close()
	// MySQL 版本停止（已注释）
	// c.db.Close()
}

// run 定时刷新协程（已注释，使用 cron 替代）
// func (c *MetadataCache) run() {
// 	ticker := time.NewTicker(c.interval)
// 	defer ticker.Stop()
//
// 	for {
// 		select {
// 		case <-c.stopCh:
// 			return
// 		case <-ticker.C:
// 			if err := c.refresh(); err != nil {
// 				log.Printf("[MetadataCache] 刷新失败: %v", err)
// 			}
// 		}
// 	}
// }

// refresh 刷新缓存（Redis 版本）
func (c *MetadataCache) refresh() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 使用 HGETALL 获取整个 Hash
	result, err := c.rdb.HGetAll(ctx, c.hashKey).Result()
	if err != nil {
		return err
	}

	newMap := make(map[int]string)
	for field, value := range result {
		adType, err := strconv.Atoi(field)
		if err != nil {
			log.Printf("[MetadataCache] 跳过无效字段: %s", field)
			continue
		}
		newMap[adType] = value
	}

	// 原子替换
	c.data.Store(newMap)
	log.Printf("[MetadataCache] 刷新完成, 共 %d 条映射", len(newMap))

	return nil
}

// refreshFromMySQL 刷新缓存（MySQL 版本，已注释）
// func (c *MetadataCache) refreshFromMySQL() error {
// 	rows, err := c.db.Query("SELECT ad_type_id, ad_type_name FROM ad_type_mapping")
// 	if err != nil {
// 		return err
// 	}
// 	defer rows.Close()
//
// 	newMap := make(map[int]string)
// 	for rows.Next() {
// 		var mapping models.AdTypeMapping
// 		if err := rows.Scan(&mapping.AdTypeID, &mapping.AdTypeName); err != nil {
// 			return err
// 		}
// 		newMap[mapping.AdTypeID] = mapping.AdTypeName
// 	}
//
// 	if err := rows.Err(); err != nil {
// 		return err
// 	}
//
// 	// 原子替换
// 	c.data.Store(newMap)
// 	log.Printf("[MetadataCache] 刷新完成, 共 %d 条映射", len(newMap))
//
// 	return nil
// }

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