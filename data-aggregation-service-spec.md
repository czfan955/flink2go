以下是对您原始提示词的优化版本，语言更清晰、结构更严谨、技术细节更准确，便于工程实现或作为开发任务说明书使用：

---

### 服务功能需求（Go 实现）

请使用 Go 编写一个高性能数据处理服务，具备以下核心能力：

---

#### 1. **Kafka 数据消费**

- 使用 [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go) 客户端从 Kafka 拉取消息。
- 配置 **Consumer Group**，支持多消费者实例协同消费。
- 启动多个 **消费者 Goroutine**（数量可配置），每个仅负责拉取消息，并将原始消息（`[]byte`）投递到一个 **共享的 Channel**（建议使用有缓冲 Channel，如 `chan []byte`），实现“拉取”与“处理”的解耦。

> 示例消息格式（JSON）：
>
> ```json
> {"minute":1,"adType":25,"brand":"redmi","show":2,"click":1,"price":0.02}
> ```

---

#### 2. **MySQL 元数据定时刷新（本地缓存）**

- 启动一个独立的后台 Goroutine。
- 使用 `time.Ticker` 每 **1 分钟** 从 MySQL 查询全量映射数据（`adType → adTypeName`）。
- 将查询结果构建成一个新的 `map[int]String`。
- 通过 `atomic.Value.Store()` 原子地替换全局共享的缓存引用，确保读操作无锁、线程安全。
- 所有后续处理逻辑通过 `atomic.Value.Load()` 获取最新缓存。

---

#### 3. **消息解析与元数据关联**

- 从 Kafka 消息 Channel 中消费消息。
- 解析 JSON，提取字段（如 `adType`, `brand`, `minute` 等）。
- 以 `adType` 为键，查询上述原子更新的本地内存 Map。
- 若命中，则用 Map 中的值对原始消息进行转换；若未命中，保持原值。

---

#### 4. **分组聚合（Sharding + Bucket 架构）**

- 聚合维度：按 `(minute, brand)` 进行分组。
- 采用 **Sharding Map + Channel** 架构，创建 **64 个聚合桶（Bucket）**。
- 对每条处理后的数据，计算其分组键的哈希值（例如：`hash(minute + "|" + brand)`），然后取模：`shardIndex = hash % 64`。
- 将该数据发送到对应 Bucket 的专属 Channel（每个 Bucket 拥有一个 `chan AggRecord`）。
- 每个 Bucket 由 **唯一的 Goroutine** 负责：
  - 持有一个本地聚合 Map（如 `map[GroupKey]AggValue`）；
  - 从自己的 Channel 读取数据并更新内部状态。

---

#### 5. **定时批量写入 ClickHouse**

- 启动一个定时器（`time.Ticker`），**每分钟触发一次 Flush**。
- Flush 流程：
  1. 向 **所有 64 个 Bucket** 发送一个特殊的 **Flush 信号**（可通过关闭 Channel、发送哨兵值或使用 `select` 多路复用实现）。
  2. 每个 Bucket 收到信号后：
     - 原子地交换内部聚合 Map（保留旧 Map 用于写入，新建空 Map 继续接收新数据）；
     - 将旧 Map 发送到 **写入协程池（Worker Pool）** 的任务队列。
  3. **写入协程池**（固定大小，如 8 个 Worker）：
     - 从任务队列消费聚合 Map；
     - 将 Map 中的数据转换为 ClickHouse 批量 Insert 语句（推荐使用 `clickhouse-go` 客户端的 `Batch` 或 `INSERT INTO ... VALUES` 批量写入）；
     - 执行写入操作，并处理错误重试/日志。

---

### 非功能性要求

- **高并发 & 低延迟**：Kafka 消费与聚合处理应尽量并行化，避免阻塞。
- **内存安全**：所有共享状态必须通过 Channel 或 atomic 操作保护，禁止直接读写共享 Map。
- **可配置性**：Kafka 地址、Topic、GroupID、MySQL DSN、ClickHouse DSN、Bucket 数量、Worker 数量等应支持通过配置文件或环境变量注入。
- **可观测性**：关键步骤应包含日志（如消费速率、Flush 触发、写入成功/失败计数），建议预留指标埋点接口（如 Prometheus）。

---

### 技术栈建议

- Kafka 客户端：`segmentio/kafka-go`
- MySQL 驱动：`go-sql-driver/mysql`
- ClickHouse 客户端：`github.com/ClickHouse/clickhouse-go/v2`
- 并发控制：Channel + Goroutine + `sync.Pool`（可选）+ `atomic.Value`
