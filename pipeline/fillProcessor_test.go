package pipeline

import (
	v1 "goInterview/config"
	"testing"
)

func TestFillProcessor_Process(t *testing.T) {
	InitBatchStore("output1.json")
	type fields struct {
		config FillConfig
	}
	type args struct {
		in <-chan v1.Data
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    <-chan v1.Data
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				config: FillConfig{
					Fields: map[string]string{
						"os_type":    "os_type",
						"os_version": "os_version",
					},
				},
			},
			args: args{
				in: Start(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &FillProcessor{
				config: tt.fields.config,
			}
			got, err := p.Process(tt.args.in)
			if err != nil {
				panic(err)
			}
			err = BatchStore(got)
			if err != nil {
				panic(err)
			}
		})
	}
	CloseBatchStore()
}
