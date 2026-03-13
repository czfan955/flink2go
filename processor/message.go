package processor

import (
	"encoding/json"
	"log"

	"flink2go/cache"
	"flink2go/models"
)

// Processor 消息处理器
type Processor struct {
	cache *cache.MetadataCache
}

// NewProcessor 创建消息处理器
func NewProcessor(cache *cache.MetadataCache) *Processor {
	return &Processor{
		cache: cache,
	}
}

// Process 处理原始消息
func (p *Processor) Process(data []byte) (*models.ProcessedMessage, error) {
	var rawMsg models.RawMessage
	if err := json.Unmarshal(data, &rawMsg); err != nil {
		log.Printf("[Processor] JSON 解析失败: %v, 数据: %s", err, string(data))
		return nil, err
	}

	// 查询元数据缓存
	adTypeName, ok := p.cache.Get(rawMsg.AdType)
	if !ok {
		adTypeName = "unknown"
	}

	processed := &models.ProcessedMessage{
		Minute:     rawMsg.Minute,
		AdType:     rawMsg.AdType,
		AdTypeName: adTypeName,
		Brand:      rawMsg.Brand,
		Show:       rawMsg.Show,
		Click:      rawMsg.Click,
		Price:      rawMsg.Price,
	}

	return processed, nil
}

// ProcessBatch 批量处理消息
func (p *Processor) ProcessBatch(dataList [][]byte) []*models.ProcessedMessage {
	results := make([]*models.ProcessedMessage, 0, len(dataList))
	for _, data := range dataList {
		msg, err := p.Process(data)
		if err != nil {
			continue
		}
		results = append(results, msg)
	}
	return results
}
