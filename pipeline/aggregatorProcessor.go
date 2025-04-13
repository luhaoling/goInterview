package pipeline

import (
	"errors"
	"fmt"
	"goInterview/backend"
	v1 "goInterview/config"
	"math"
	"strings"
	"sync"
	"time"
)

// AggregatorProcessor 聚合数据
type AggregatorProcessor struct {
	config  AggregatorConfig
	mu      sync.Mutex
	done    chan struct{}
	backend backend.Backend
}

type AggregatorConfig struct {
	GroupByFields []string
	ValueField    string
	Interval      time.Duration
	Fields        []string
}

func NewAggregatorProcessor(config AggregatorConfig, backend backend.Backend) *AggregatorProcessor {
	return &AggregatorProcessor{
		config:  config,
		done:    make(chan struct{}),
		backend: backend,
	}
}

func (p *AggregatorProcessor) Process(in <-chan v1.Data) (<-chan v1.Data, error) {
	out := make(chan v1.Data)
	group := make(map[string][]v1.Data)
	ticker := time.NewTicker(p.config.Interval)
	var err error

	go func() {
		defer close(out)
		defer ticker.Stop()

		for {
			select {
			case <-p.done:
				err = p.flushToBackend(group)
				return

			case <-ticker.C:
				p.mu.Lock()
				if len(group) > 0 {
					err = p.flushToBackend(group)
					group = make(map[string][]v1.Data)
				}
				p.mu.Unlock()

			case data, ok := <-in:
				if !ok {
					p.mu.Lock()
					err = p.flushToBackend(group)
					p.mu.Unlock()
					return
				}

				p.mu.Lock()
				key := p.getGroupKey(data)
				group[key] = append(group[key], data)
				p.mu.Unlock()
			}
		}
	}()

	return out, err
}

func (p *AggregatorProcessor) flushToBackend(group map[string][]v1.Data) error {
	if len(group) == 0 {
		return errors.New("group is nil")
	}

	var allAggregated []v1.Data
	for key := range group {
		aggregated := p.aggregateGroup(group[key])
		allAggregated = append(allAggregated, aggregated...)
	}

	// 创建一个新的channel用于批量发送
	batchChan := make(chan []v1.Data, 1)
	batchChan <- allAggregated
	close(batchChan)

	if err := p.backend.BatchStore(batchChan); err != nil {
		return err
	}
	return nil
}

func (p *AggregatorProcessor) aggregateGroup(group []v1.Data) []v1.Data {
	if len(group) == 0 {
		return nil
	}

	ag := struct {
		sum   float64
		count float64
		min   float64
		max   float64
	}{
		min: math.Inf(1),
		max: math.Inf(-1),
	}

	for _, data := range group {
		value := getFloat(data, p.config.ValueField)
		ag.sum += value
		ag.count++
		ag.min = math.Min(ag.min, value)
		ag.max = math.Max(ag.max, value)
	}

	var results []v1.Data
	record := group[0]

	for _, field := range p.config.Fields {
		da := make(v1.Data, len(record))
		for k := range record {
			da[k] = record[k]
		}

		delete(da, p.config.ValueField)

		var value float64
		switch field {
		case "sum":
			value = ag.sum
		case "count":
			value = ag.count
		case "min":
			value = ag.min
		case "max":
			value = ag.max
		}

		key := fmt.Sprintf("%s_%s", p.config.ValueField, field)
		da[key] = value
		results = append(results, da)
	}

	return results
}

func (p *AggregatorProcessor) getGroupKey(data v1.Data) string {
	var keyParts []string
	for _, field := range p.config.GroupByFields {
		if val, ok := data[field]; ok {
			keyParts = append(keyParts, fmt.Sprintf("%v", val))
		}
	}
	return strings.Join(keyParts, "|")
}

func (p *AggregatorProcessor) Stop() {
	close(p.done)
}

func getFloat(data v1.Data, field string) float64 {
	if val, ok := data[field].(float64); ok {
		return val
	}
	return 0
}
