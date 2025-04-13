package backend

import (
	"goInterview/config"
)

// ========== 后端实现 ==========

const (
	File int = iota + 1
	MySQL
)

// Backend 接口定义了数据存储后端的行
type Backend interface {
	BatchStore(<-chan []config.Data) error
	Start() error
	Stop() error
}

func NewBackend(config config.Backend) (Backend, error) {
	switch config.Type {
	case File:
		return NewFileBackend(config)
	case MySQL:
		return NewMySQLBackend(config.DNS, config.BatchSize)
	default:
		panic("unknown data source type")
	}
}
