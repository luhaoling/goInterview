package main

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"goInterview/backend"
	"goInterview/config"
	pipeline2 "goInterview/pipeline"
	"goInterview/receiver"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func Start(conf config.Config) {
	// 创建接收器
	receiver := receiver.NewDataSource(conf.Receiver)

	// 创建后端
	backend, err := backend.NewBackend(conf.Backend)
	if err != nil {
		panic(err)
	}

	// 设置信号处理，在接收到中断信号时停止程序
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopChan
		log.Println("Shutting down...")
		backend.Stop()
		os.Exit(0)
	}()

	// 创建处理器
	processors := []pipeline2.Processor{
		pipeline2.NewFilterProcessor(pipeline2.FilterConfig{
			Field:  conf.Processor.FilterConfig.Field,
			Values: conf.Processor.FilterConfig.Values,
			Open:   conf.Processor.FilterConfig.Open,
		}),
		pipeline2.NewFillProcessor(pipeline2.FillConfig{
			Fields: conf.Processor.FillConfig.Field,
			Open:   conf.Processor.FillConfig.Open,
		}),
		pipeline2.NewAggregatorProcessor(pipeline2.AggregatorConfig{
			GroupByFields: conf.Processor.AggregatorConfig.GroupByFields,
			ValueField:    conf.Processor.AggregatorConfig.ValueField,
			Interval:      conf.Processor.AggregatorConfig.Interval,
			Fields:        conf.Processor.AggregatorConfig.Fields,
		}, backend),
	}

	// 创建并运行管道
	pipe := pipeline2.NewPipeline(receiver, backend, processors, conf.Pipeline.BatchSize, runtime.NumCPU())
	if err := pipe.Run(); err != nil {
		log.Fatalf("Pipeline error: %v", err)
	}
}

func main() {
	conf := InitConfig()
	Start(conf)
}

func InitConfig() config.Config {
	var conf = new(config.Config)
	viper.SetConfigFile("config/config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	if err := viper.Unmarshal(conf); err != nil {
		panic(fmt.Errorf("unmarshal conf failed, err:%s \n", err))
	}

	// 监控配置文件变化
	viper.WatchConfig()
	viper.OnConfigChange(func(in fsnotify.Event) {
		fmt.Println("配置文件已修改...")
		if err := viper.Unmarshal(conf); err != nil {
			panic(fmt.Errorf("unmarshal conf failed, err:%s \n", err))
		}
	})
	return *conf
}
