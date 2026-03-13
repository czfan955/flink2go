package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 全局配置结构
type Config struct {
	Kafka      KafkaConfig      `yaml:"kafka"`
	MySQL      MySQLConfig      `yaml:"mysql"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Aggregator AggregatorConfig `yaml:"aggregator"`
	Writer     WriterConfig     `yaml:"writer"`
}

// KafkaConfig Kafka 配置
type KafkaConfig struct {
	Brokers       []string `yaml:"brokers"`
	Topic         string   `yaml:"topic"`
	GroupID       string   `yaml:"group_id"`
	ConsumerCount int      `yaml:"consumer_count"`
	ChannelSize   int      `yaml:"channel_size"`
}

// MySQLConfig MySQL 配置
type MySQLConfig struct {
	DSN             string        `yaml:"dsn"`
	RefreshInterval time.Duration `yaml:"refresh_interval"`
}

// ClickHouseConfig ClickHouse 配置
type ClickHouseConfig struct {
	Addr     string `yaml:"addr"`
	Database string `yaml:"database"`
	Table    string `yaml:"table"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// AggregatorConfig 聚合器配置
type AggregatorConfig struct {
	BucketCount int `yaml:"bucket_count"`
	ChannelSize int `yaml:"channel_size"`
}

// WriterConfig 写入器配置
type WriterConfig struct {
	WorkerCount   int           `yaml:"worker_count"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	BatchSize     int           `yaml:"batch_size"`
}

// Load 从文件加载配置
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// 设置默认值
	setDefaults(&cfg)

	return &cfg, nil
}

// setDefaults 设置默认配置值
func setDefaults(cfg *Config) {
	if cfg.Kafka.ConsumerCount <= 0 {
		cfg.Kafka.ConsumerCount = 4
	}
	if cfg.Kafka.ChannelSize <= 0 {
		cfg.Kafka.ChannelSize = 10000
	}
	if cfg.MySQL.RefreshInterval <= 0 {
		cfg.MySQL.RefreshInterval = time.Minute
	}
	if cfg.Aggregator.BucketCount <= 0 {
		cfg.Aggregator.BucketCount = 64
	}
	if cfg.Aggregator.ChannelSize <= 0 {
		cfg.Aggregator.ChannelSize = 1000
	}
	if cfg.Writer.WorkerCount <= 0 {
		cfg.Writer.WorkerCount = 8
	}
	if cfg.Writer.FlushInterval <= 0 {
		cfg.Writer.FlushInterval = time.Minute
	}
	if cfg.Writer.BatchSize <= 0 {
		cfg.Writer.BatchSize = 10000
	}
}
