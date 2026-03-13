package api

import (
	"net/http"
	"runtime"
	"time"

	"flink2go/aggregator"
	"flink2go/cache"
	"flink2go/config"

	"github.com/gin-gonic/gin"
)

// Handler HTTP 处理器
type Handler struct {
	config     *config.Config
	cache      *cache.MetadataCache
	aggregator *aggregator.Aggregator
	startTime  time.Time
}

// NewHandler 创建 HTTP 处理器
func NewHandler(cfg *config.Config, cache *cache.MetadataCache, agg *aggregator.Aggregator) *Handler {
	return &Handler{
		config:     cfg,
		cache:      cache,
		aggregator: agg,
		startTime:  time.Now(),
	}
}

// SetupRouter 设置路由
func SetupRouter(h *Handler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// 健康检查
	r.GET("/health", h.Health)

	// 服务状态
	r.GET("/status", h.Status)

	// 配置信息
	r.GET("/config", h.GetConfig)

	// 元数据缓存
	r.GET("/cache/metadata", h.GetMetadataCache)
	r.POST("/cache/refresh", h.RefreshMetadataCache)

	// 手动触发 Flush
	r.POST("/flush", h.TriggerFlush)

	// 指标接口
	r.GET("/metrics", h.Metrics)

	return r
}

// Health 健康检查
func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}

// Status 服务状态
func (h *Handler) Status(c *gin.Context) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	c.JSON(http.StatusOK, gin.H{
		"status":       "running",
		"uptime":       time.Since(h.startTime).String(),
		"go_version":   runtime.Version(),
		"goroutines":   runtime.NumGoroutine(),
		"memory_mb":    m.Alloc / 1024 / 1024,
		"sys_memory_mb": m.Sys / 1024 / 1024,
	})
}

// GetConfig 获取配置
func (h *Handler) GetConfig(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"kafka":       h.config.Kafka,
		"mysql":       h.config.MySQL,
		"clickhouse":  h.config.ClickHouse,
		"aggregator":  h.config.Aggregator,
		"writer":      h.config.Writer,
	})
}

// GetMetadataCache 获取元数据缓存
func (h *Handler) GetMetadataCache(c *gin.Context) {
	data := h.cache.GetAll()
	c.JSON(http.StatusOK, gin.H{
		"count": len(data),
		"data":  data,
	})
}

// RefreshMetadataCache 刷新元数据缓存
func (h *Handler) RefreshMetadataCache(c *gin.Context) {
	// 这里可以添加手动刷新逻辑
	c.JSON(http.StatusOK, gin.H{
		"message": "refresh signal sent",
	})
}

// TriggerFlush 手动触发 Flush
func (h *Handler) TriggerFlush(c *gin.Context) {
	h.aggregator.Flush()
	c.JSON(http.StatusOK, gin.H{
		"message": "flush triggered",
	})
}

// Metrics 指标接口
func (h *Handler) Metrics(c *gin.Context) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	c.JSON(http.StatusOK, gin.H{
		"uptime_seconds":    time.Since(h.startTime).Seconds(),
		"goroutines":        runtime.NumGoroutine(),
		"memory_alloc_mb":   m.Alloc / 1024 / 1024,
		"memory_total_mb":   m.TotalAlloc / 1024 / 1024,
		"memory_sys_mb":     m.Sys / 1024 / 1024,
		"heap_objects":      m.HeapObjects,
		"gc_cycles":         m.NumGC,
	})
}
