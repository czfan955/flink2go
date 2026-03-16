package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flink2go/api"
	"flink2go/app"
	"flink2go/scheduler"
)

func main() {
	// 创建应用实例
	application, err := app.New("config.yaml")
	if err != nil {
		log.Fatalf("创建应用失败: %v", err)
	}

	// 创建上下文
	/**
	在 main 函数中创建一个 可取消的上下文 （Context），具体含义是：	
	- context.Background() ：创建一个根上下文（根 Context），这是所有上下文的起点，通常用于整个应用的生命周期
	- context.WithCancel() ：基于根上下文创建一个可取消的上下文，并返回两个值：
  	- ctx ：新创建的可取消上下文对象
  	- cancel ：一个函数，调用它可以取消这个上下文及其派生的所有上下文
	**/
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化应用组件
	if err := application.Init(ctx); err != nil {
		log.Fatalf("初始化应用失败: %v", err)
	}

	// 启动消息处理
	application.Start(ctx)

	// 创建定时任务调度器
	sched := scheduler.NewScheduler()
	if err := sched.AddFlushTask(application.GetConfig().Writer.FlushCron, application.GetAggregator()); err != nil {
		log.Fatalf("添加 Flush 定时任务失败: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// 启动 Web 服务
	srv := startHTTPServer(application)

	// 等待退出信号
	waitForShutdown(cancel, application, srv)
}

// startHTTPServer 启动 HTTP 服务
func startHTTPServer(application *app.App) *http.Server {
	handler := api.NewHandler(application.GetConfig(), application.GetCache(), application.GetAggregator())
	router := api.SetupRouter(handler)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		log.Println("Web 服务启动在 :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Web 服务启动失败: %v", err)
		}
	}()

	return srv
}

// waitForShutdown 等待关闭信号
func waitForShutdown(cancel context.CancelFunc, application *app.App, srv *http.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("收到退出信号，正在关闭服务...")

	// 优雅关闭 Web 服务
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)

	cancel()
	application.Stop()
	time.Sleep(time.Second * 2)
	log.Println("服务已关闭")
}
