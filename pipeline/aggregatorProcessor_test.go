package pipeline

import (
	"fmt"
	"goInterview/backend"
	v1 "goInterview/config"
	"sync"
	"testing"
	"time"
)

func TestAggregatorProcessor_Process(t *testing.T) {
	InitBatchStore("ouput2.json")
	type fields struct {
		config  AggregatorConfig
		mu      sync.Mutex
		ticker  *time.Ticker
		done    chan struct{}
		backend backend.Backend
	}
	type args struct {
		in <-chan v1.Data
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    <-chan []v1.Data
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				config: AggregatorConfig{
					GroupByFields: []string{"zone", "bizid", "env", "os_type", "os_version"},
					ValueField:    "",
				},
				mu:      sync.Mutex{},
				ticker:  nil,
				done:    nil,
				backend: nil,
			},
			args: args{
				in: Start(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &AggregatorProcessor{
				config:  tt.fields.config,
				mu:      tt.fields.mu,
				ticker:  tt.fields.ticker,
				done:    tt.fields.done,
				backend: tt.fields.backend,
			}
			got, err := p.Process(tt.args.in)
			if err != nil {
				panic(err)
			}
			err = BatchStore1(got)
			if err != nil {
				panic(err)
			}
		})
	}
	CloseBatchStore()
}

func BatchStore1(data <-chan []v1.Data) error {
	if !initialized {
		return fmt.Errorf("batch store not initialized")
	}

	batch := make([]v1.Data, 0, batchSize)

	for item := range data {
		for _, d := range item {
			batch = append(batch, d)

			if len(batch) >= batchSize {
				if err := writeBatch(batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		return writeBatch(batch)
	}

	return nil
}
