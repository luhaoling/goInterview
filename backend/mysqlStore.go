package backend

import (
	"fmt"
	v1 "goInterview/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"sync"
	"time"
)

// Metric 定义数据表结构
type Metric struct {
	ID         uint           `gorm:"primaryKey"`
	Zone       string         `gorm:"size:50;not null;index:idx_zone"`
	BizID      string         `gorm:"size:50;not null;index:idx_bizid" column:"bizid"`
	Env        string         `gorm:"size:50;not null;index:idx_env"`
	OSType     string         `gorm:"size:50;not null;index:idx_os_type" column:"os_type"`
	OSVersion  string         `gorm:"size:100;not null" column:"os_version"`
	ValueSum   float64        `gorm:"type:decimal(20,2)" column:"__value__sum"`
	ValueCount int            `gorm:"type:decimal(20,2)" column:"__value__count"`
	ValueMin   float64        `gorm:"type:decimal(20,2)" column:"__value__min"`
	ValueMax   float64        `gorm:"type:decimal(20,2)" column:"__value__max"`
	CreatedAt  time.Time      `gorm:"autoCreateTime"`
	UpdatedAt  time.Time      `gorm:"autoUpdateTime"`
	DeletedAt  gorm.DeletedAt `gorm:"index"`
}

func (Metric) TableName() string {
	return "metrics"
}

// MySQLBackend 实现 Backend 接口
type MySQLBackend struct {
	db          *gorm.DB
	batchConfig BatchConfig
	stopChan    chan struct{}
	wg          sync.WaitGroup
	mu          sync.Mutex
	batchBuffer []Metric
	ticker      *time.Ticker
}

func NewMySQLBackend(dsn string, config BatchConfig) (*MySQLBackend, error) {
	// 设置默认值
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 5 * time.Second
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}

	// 配置 GORM
	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	}

	// 建立数据库连接
	db, err := gorm.Open(mysql.Open(dsn), gormConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// 配置连接池
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	backend := &MySQLBackend{
		db:          db,
		batchConfig: config,
		stopChan:    make(chan struct{}),
		batchBuffer: make([]Metric, 0, config.BatchSize),
		ticker:      time.NewTicker(config.FlushInterval),
	}

	// 启动后台刷新协程
	go backend.backgroundFlush()

	return backend, nil
}

func (m *MySQLBackend) Start() error {
	// 自动迁移表结构
	if err := m.db.AutoMigrate(&Metric{}); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}
	return nil
}

func (m *MySQLBackend) BatchStore(dataChan <-chan []v1.Data) error {
	m.wg.Add(1)
	defer m.wg.Done()

	for {
		select {
		case <-m.stopChan:
			return nil
		case batch, ok := <-dataChan:
			if !ok {
				// 通道关闭，处理剩余数据
				m.Flush()
				return nil
			}

			// 转换数据并添加到缓冲区
			metrics, err := m.convertBatch(batch)
			if err != nil {
				log.Printf("Failed to convert batch: %v", err)
				continue
			}

			m.mu.Lock()
			m.batchBuffer = append(m.batchBuffer, metrics...)
			shouldFlush := len(m.batchBuffer) >= m.batchConfig.BatchSize
			m.mu.Unlock()

			if shouldFlush {
				if err := m.Flush(); err != nil {
					log.Printf("Flush failed: %v", err)
				}
			}
		}
	}
}

func (m *MySQLBackend) Store(data v1.Data) error {
	metric, err := convertToMetric(data)
	if err != nil {
		return fmt.Errorf("conversion failed: %w", err)
	}

	m.mu.Lock()
	m.batchBuffer = append(m.batchBuffer, metric)
	shouldFlush := len(m.batchBuffer) >= m.batchConfig.BatchSize
	m.mu.Unlock()

	if shouldFlush {
		return m.Flush()
	}
	return nil
}

func (m *MySQLBackend) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.batchBuffer) == 0 {
		return nil
	}

	// 复制缓冲区以避免长时间持有锁
	toInsert := make([]Metric, len(m.batchBuffer))
	copy(toInsert, m.batchBuffer)
	m.batchBuffer = m.batchBuffer[:0]

	// 重试逻辑
	var err error
	for i := 0; i < m.batchConfig.MaxRetries; i++ {
		if err = m.db.CreateInBatches(toInsert, m.batchConfig.BatchSize).Error; err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond) // 指数退避
	}

	return fmt.Errorf("after %d retries, still failed to insert batch: %w", m.batchConfig.MaxRetries, err)
}

func (m *MySQLBackend) backgroundFlush() {
	for {
		select {
		case <-m.ticker.C:
			if err := m.Flush(); err != nil {
				log.Printf("Background flush failed: %v", err)
			}
		case <-m.stopChan:
			m.ticker.Stop()
			return
		}
	}
}

func (m *MySQLBackend) convertBatch(batch []v1.Data) ([]Metric, error) {
	metrics := make([]Metric, 0, len(batch))
	for _, data := range batch {
		metric, err := convertToMetric(data)
		if err != nil {
			return nil, fmt.Errorf("convert failed: %w", err)
		}
		metrics = append(metrics, metric)
	}
	return metrics, nil
}

func convertToMetric(data v1.Data) (Metric, error) {
	var metric Metric
	var ok bool
	var floatVal float64

	// 提取基础维度字段
	if metric.Zone, ok = data["zone"].(string); !ok {
		return metric, fmt.Errorf("invalid zone value")
	}
	if metric.BizID, ok = data["bizid"].(string); !ok {
		return metric, fmt.Errorf("invalid bizid value")
	}
	if metric.Env, ok = data["env"].(string); !ok {
		return metric, fmt.Errorf("invalid env value")
	}
	if metric.OSType, ok = data["os_type"].(string); !ok {
		return metric, fmt.Errorf("invalid os_type value")
	}
	if metric.OSVersion, ok = data["os_version"].(string); !ok {
		return metric, fmt.Errorf("invalid os_version value")
	}

	// 提取指标字段
	if floatVal, ok = data["__value__sum"].(float64); ok {
		metric.ValueSum = floatVal
	}
	if floatVal, ok = data["__value__min"].(float64); ok {
		metric.ValueMin = floatVal
	}
	if floatVal, ok = data["__value__max"].(float64); ok {
		metric.ValueMax = floatVal
	}
	if intVal, ok := data["__value__count"].(int); ok {
		metric.ValueCount = intVal
	} else if floatVal, ok = data["__value__count"].(float64); ok {
		metric.ValueCount = int(floatVal)
	}

	metric.CreatedAt = time.Now()
	metric.UpdatedAt = time.Now()

	return metric, nil
}

func (m *MySQLBackend) Stop() error {
	close(m.stopChan)
	m.wg.Wait()

	// 确保所有数据都已刷新
	if err := m.Flush(); err != nil {
		log.Printf("Final flush failed: %v", err)
	}

	sqlDB, err := m.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}
	return sqlDB.Close()
}

var _ Backend = (*MySQLBackend)(nil)
