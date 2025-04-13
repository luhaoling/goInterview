package config

import "time"

type Config struct {
	Backend   `mapstructure:"backend"`
	Processor `mapstructure:"processor"`
	Pipeline  `mapstructure:"pipeline"`
	Receiver  `mapstructure:"receiver"`
}

type Processor struct {
	FilterConfig     `mapstructure:"filterConfig"`
	FillConfig       `mapstructure:"fillConfig"`
	AggregatorConfig `mapstructure:"aggregatorConfig"`
}

type FilterConfig struct {
	Field  string   `mapstructure:"field"`
	Values []string `mapstructure:"values"`
	Open   bool     `mapstructure:"open"`
}

type FillConfig struct {
	Field []string `mapstructure:"field"`
	Open  bool     `mapstructure:"open"`
}

type AggregatorConfig struct {
	GroupByFields []string      `mapstructure:"groupByFields"`
	ValueField    string        `mapstructure:"valueField"`
	Interval      time.Duration `mapstructure:"interval"`
	Fields        []string      `mapstructure:"fields"`
}

type Pipeline struct {
	BatchSize int `mapstructure:"batchSize"`
}

type Receiver struct {
	Type     int      `mapstructure:"type"`
	Path     []string `mapstructure:"path"`
	ChanSize int64    `mapstructure:"chanSize"`
}

type Backend struct {
	Path          string        `mapstructure:"path"`
	Type          int           `mapstructure:"type"`
	DNS           string        `mapstructure:"dns"`
	BatchSize     int           `mapstructure:"batchSize"`
	FlushInterval time.Duration `mapstructure:"flushInterval"`
	BufferSize    int           `mapstructure:"bufferSize"`
}
