package scheduler

import (
	"log"

	"flink2go/aggregator"

	"github.com/robfig/cron/v3"
)

// Scheduler 定时任务调度器
type Scheduler struct {
	cron *cron.Cron
}

// NewScheduler 创建定时任务调度器
func NewScheduler() *Scheduler {
	return &Scheduler{
		cron: cron.New(cron.WithSeconds()),
	}
}

// AddFlushTask 添加 Flush 定时任务
func (s *Scheduler) AddFlushTask(cronExpr string, agg *aggregator.Aggregator) error {
	_, err := s.cron.AddFunc(cronExpr, func() {
		agg.Flush()
	})
	if err != nil {
		return err
	}
	log.Printf("[Scheduler] Flush 定时任务已添加，cron: %s", cronExpr)
	return nil
}

// Start 启动调度器
func (s *Scheduler) Start() {
	s.cron.Start()
	log.Println("[Scheduler] 调度器已启动")
}

// Stop 停止调度器
func (s *Scheduler) Stop() {
	s.cron.Stop()
	log.Println("[Scheduler] 调度器已停止")
}
