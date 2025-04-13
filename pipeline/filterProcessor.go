package pipeline

import (
	"fmt"
	v1 "goInterview/config"
)

// ========== 处理器实现 ==========

// FilterProcessor 过滤数据
type FilterProcessor struct {
	config FilterConfig
}

type FilterConfig struct {
	Field  string
	Values []string
	Open   bool
}

func NewFilterProcessor(config FilterConfig) *FilterProcessor {
	return &FilterProcessor{config: config}
}

func (p *FilterProcessor) Process(in <-chan v1.Data) (<-chan v1.Data, error) {
	if !p.config.Open {
		return in, nil
	}
	out := make(chan v1.Data)

	go func() {
		defer close(out)
		for data := range in {
			value, exists := data[p.config.Field]
			if !exists {
				continue
			}

			for _, v := range p.config.Values {
				if v == value {
					fmt.Println("filter:", data)
					out <- data
				}
			}
		}
	}()

	return out, nil // 过滤掉不符合条件的数据
}
