# Flink2Go - 高性能数据聚合服务

基于 Go 语言实现的高性能数据聚合服务，从 Kafka 消费数据，进行实时聚合后写入 ClickHouse。

## 架构设计

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Kafka     │────▶│   Processor  │────▶│  Aggregator │
│  Consumer   │     │   (解析+关联) │     │  (64 Bucket)│
└─────────────┘     └──────────────┘     └─────────────┘
                                                │
┌─────────────┐                                 ▼
│    MySQL    │──────▶ [元数据缓存] ──────▶ ┌─────────────┐
│ (AdType映射)│       (atomic.Value)        │ ClickHouse  │
└─────────────┘                            │   Writer    │
                                           └─────────────┘
```

## 核心特性

### 1. Kafka 数据消费
- 使用 `segmentio/kafka-go` 客户端
- 支持 Consumer Group 多实例协同消费
- 多 Goroutine 并行拉取，通过共享 Channel 解耦

### 2. MySQL 元数据缓存
- 独立后台协程定时刷新（默认 1 分钟）
- 使用 `atomic.Value` 实现无锁读取
- 线程安全的缓存替换

### 3. 分组聚合（Sharding + Bucket）
- 聚合维度：`(minute, brand)`
- 64 个聚合桶并行处理
- FNV 哈希分片，确保数据均匀分布

### 4. ClickHouse 批量写入
- 定时 Flush（默认 1 分钟）
- Worker Pool 并发写入（默认 8 个协程）
- 批量 Insert 优化性能

### 5. Web 服务（Gin）
- 健康检查、状态监控接口
- 手动触发 Flush
- 配置查询、缓存查询

## 项目结构

```
flink2go/
├── cmd/main.go           # 主程序入口
├── api/handler.go        # HTTP 接口处理器
├── config/config.go      # 配置管理
├── models/models.go      # 数据模型
├── cache/metadata.go     # MySQL 元数据缓存
├── consumer/kafka.go     # Kafka 消费者
├── processor/message.go  # 消息解析与处理
├── aggregator/aggregator.go  # 分片聚合器
├── writer/clickhouse.go  # ClickHouse 写入器
├── config.yaml           # 配置文件
└── go.mod
```

## 配置说明

```yaml
# Kafka 配置
kafka:
  brokers:
    - "localhost:9092"
  topic: "ad-events"
  group_id: "flink2go-consumer-group"
  consumer_count: 4          # 消费者协程数量
  channel_size: 10000        # 消息通道缓冲大小

# MySQL 配置
mysql:
  dsn: "user:password@tcp(localhost:3306)/ad_db?parseTime=true"
  refresh_interval: 1m       # 缓存刷新间隔

# ClickHouse 配置
clickhouse:
  addr: "localhost:9000"
  database: "ad_analytics"
  table: "ad_aggregation"
  username: "default"
  password: ""

# 聚合器配置
aggregator:
  bucket_count: 64           # 聚合桶数量
  channel_size: 1000         # 每个桶的通道缓冲

# 写入器配置
writer:
  worker_count: 8            # 写入协程池大小
  flush_interval: 1m         # Flush 间隔
  batch_size: 10000          # 批量写入大小
```

## 数据模型

### Kafka 消息格式
```json
{
  "minute": 1,
  "adType": 25,
  "brand": "redmi",
  "show": 2,
  "click": 1,
  "price": 0.02
}
```

### ClickHouse 表结构
```sql
CREATE TABLE ad_aggregation (
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
PARTITION BY toYYYYMM(toDateTime(minute * 60));
```

## HTTP 接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/status` | GET | 服务状态 |
| `/config` | GET | 获取配置 |
| `/cache/metadata` | GET | 获取元数据缓存 |
| `/cache/refresh` | POST | 刷新元数据缓存 |
| `/flush` | POST | 手动触发 Flush |
| `/metrics` | GET | 服务指标 |

## 快速开始

### 1. 安装依赖
```bash
go mod tidy
```

### 2. 修改配置
编辑 `config.yaml`，配置 Kafka、MySQL、ClickHouse 连接信息。

### 3. 编译运行
```bash
go build -o flink2go ./cmd/main.go
./flink2go
```

服务启动后监听 `:8080` 端口。

### 4. 验证服务
```bash
curl http://localhost:8080/health
curl http://localhost:8080/status
```

## 技术栈

- **Kafka 客户端**: `github.com/segmentio/kafka-go`
- **MySQL 驱动**: `github.com/go-sql-driver/mysql`
- **ClickHouse 客户端**: `github.com/ClickHouse/clickhouse-go/v2`
- **Web 框架**: `github.com/gin-gonic/gin`
- **配置解析**: `gopkg.in/yaml.v3`

## 并发安全

- 所有共享状态通过 Channel 或 `atomic.Value` 保护
- 禁止直接读写共享 Map
- 每个 Bucket 由唯一 Goroutine 负责，避免锁竞争

## 可观测性

- 关键步骤包含日志（消费速率、Flush 触发、写入结果）
- `/metrics` 接口提供运行时指标
- 预留 Prometheus 指标埋点接口
