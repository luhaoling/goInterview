package pipeline

import (
	v1 "goInterview/config"

	"goInterview/backend"
	"goInterview/receiver"
	"log"
	"sync"
	"time"
)

// Processor 接口定义了数据处理器的行为
type Processor interface {
	Process(<-chan v1.Data) (<-chan v1.Data, error)
}

// Pipeline 管理处理器链和数据流
type Pipeline struct {
	processors []Processor
	receiver   receiver.Receiver
	backend    backend.Backend
	batchSize  int
	workers    int
	interval   time.Duration
}

// NewPipeline 创建一个新的处理管道
func NewPipeline(receiver receiver.Receiver, backend backend.Backend, processors []Processor, batchSize, workers int) *Pipeline {
	return &Pipeline{
		processors: processors,
		receiver:   receiver,
		backend:    backend,
		batchSize:  batchSize,
		workers:    workers,
	}
}

// Run 启动管道处理
func (p *Pipeline) Run() error {
	// 启动接收器和后端
	if err := p.receiver.Start(); err != nil {
		return err
	}
	if err := p.backend.Start(); err != nil {
		return err
	}

	dataChan := p.receiver.Receive()
	var wg sync.WaitGroup

	// 使用一个缓冲区来积累数据
	buffer := make([]v1.Data, 0, p.batchSize)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case data, ok := <-dataChan:
			if !ok {
				// 通道关闭，处理剩余数据
				if len(buffer) > 0 {
					wg.Add(1)
					go func(batch []v1.Data) {
						defer wg.Done()
						if err := p.processAndStoreBatch(batch); err != nil {
							log.Printf("Error processing final batch: %v", err)
						}
					}(buffer)
				}
				wg.Wait() // 等待所有 goroutine 完成
				return p.backend.Stop()
			}

			// 积累数据
			buffer = append(buffer, data)

			// 当积累到足够数量时，启动 goroutine 处理
			if len(buffer) >= p.batchSize {
				wg.Add(1)
				go func(batch []v1.Data) {
					defer wg.Done()
					if err := p.processAndStoreBatch(batch); err != nil {
						log.Printf("Error processing batch: %v", err)
					}
				}(buffer)
				buffer = make([]v1.Data, 0, p.batchSize) // 重置缓冲区
			}

		case <-ticker.C:
			// 定时检查，避免数据滞留
			if len(buffer) > 0 {
				wg.Add(1)
				go func(batch []v1.Data) {
					defer wg.Done()
					if err := p.processAndStoreBatch(batch); err != nil {
						log.Printf("Error processing timed batch: %v", err)
					}
				}(buffer)
				buffer = make([]v1.Data, 0, p.batchSize) // 重置缓冲区
			}
		}
	}
}

func (p *Pipeline) processAndStoreBatch(batch []v1.Data) error {
	in := ProcessorIn(batch) // 初始输入
	var err error

	// 依次调用所有处理器
	for _, processor := range p.processors {
		in, err = processor.Process(in)
		if err != nil {
			return err
		}
	}
	for {
		select {
		case <-in:
			return nil
		}
	}

	return nil
}

func ProcessorIn(batch []v1.Data) <-chan v1.Data {
	out := make(chan v1.Data, 100)
	go func() {
		defer close(out)
		for _, d := range batch {
			out <- d
		}
	}()
	return out
}
