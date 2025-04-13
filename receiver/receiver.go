package receiver

import (
	"goInterview/config"
)

const (
	File int = iota + 1
	HTTP
)

// Receiver 接口定义了数据接收器的行为
type Receiver interface {
	Receive() <-chan config.Data
	Start() error
	Stop() error
}

func NewDataSource(config config.Receiver) Receiver {
	switch config.Type {
	case File:
		return NewFileReceiver(config)
	case HTTP:
		return NewHTTPReceiver(config)
	default:
		panic("unknown data source type")
	}
}
