package backend

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	v1 "goInterview/config"
	"io"
	"os"
	"sync"
	"time"
)

type BatchConfig struct {
	BatchSize     int
	FlushInterval time.Duration
	BufferSize    int
}

// FileBackend 将数据存储到文件
type FileBackend struct {
	filePath     string
	fileWriter   *bufio.Writer
	fileHandle   *os.File
	fileEncoder  *json.Encoder
	filePos      int64
	writeCounter int
	needsComma   bool
	mu           sync.Mutex
	initialized  bool
	originalSize int64

	// 批处理相关字段
	batchBuffer   []v1.Data
	batchConfig   BatchConfig
	flushTicker   *time.Ticker
	flushStopChan chan struct{}
}

func NewFileBackend(conf v1.Backend) (*FileBackend, error) {
	return &FileBackend{
		filePath: conf.Path,
		batchConfig: BatchConfig{
			BatchSize:     conf.BatchSize,
			FlushInterval: conf.FlushInterval,
			BufferSize:    conf.BufferSize,
		},
		batchBuffer:   make([]v1.Data, 0, conf.BatchSize),
		flushStopChan: make(chan struct{}),
	}, nil
}

func (b *FileBackend) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.initialized {
		return nil
	}

	file, err := os.OpenFile(b.filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	b.fileHandle = file
	b.fileWriter = bufio.NewWriterSize(file, b.batchConfig.BufferSize)
	b.fileEncoder = json.NewEncoder(b.fileWriter)
	b.writeCounter = 0
	b.needsComma = false

	// 获取文件原始大小和内容
	b.originalSize, err = b.fileHandle.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to get file size: %w", err)
	}

	// 处理空文件情况
	if b.originalSize == 0 {
		if _, err := b.fileWriter.WriteString("[\n]"); err != nil {
			file.Close()
			return fmt.Errorf("failed to write empty array: %w", err)
		}

		if err := b.fileWriter.Flush(); err != nil {
			file.Close()
			return fmt.Errorf("failed to flush empty array: %w", err)
		}

		if _, err := b.fileHandle.Seek(1, io.SeekStart); err != nil {
			file.Close()
			return fmt.Errorf("failed to seek after empty array: %w", err)
		}

		b.needsComma = false
	} else {
		if err := b.validateAndPrepareExistingFile(); err != nil {
			file.Close()
			return fmt.Errorf("invalid existing content: %w", err)
		}
	}

	// 获取当前写入位置
	b.filePos, err = b.fileHandle.Seek(0, io.SeekCurrent)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// 启动定时刷新协程
	b.flushTicker = time.NewTicker(b.batchConfig.FlushInterval)
	go b.backgroundFlush()

	b.initialized = true
	return nil
}

func (b *FileBackend) backgroundFlush() {
	for {
		select {
		case <-b.flushTicker.C:
			b.Flush()
		case <-b.flushStopChan:
			return
		}
	}
}

// BatchStore 实现批量存储接口
func (b *FileBackend) BatchStore(data <-chan []v1.Data) error {
	if !b.initialized {
		return fmt.Errorf("batch store not initialized")
	}

	for batch := range data {
		b.mu.Lock()
		b.batchBuffer = append(b.batchBuffer, batch...)
		b.mu.Unlock()

		// 检查是否达到批量大小
		if len(b.batchBuffer) >= b.batchConfig.BatchSize {
			if err := b.Flush(); err != nil {
				return err
			}
		}
	}

	// 处理剩余数据
	return b.Flush()
}

// Store 单条数据存储，内部使用批处理缓冲
func (b *FileBackend) Store(data v1.Data) error {
	if !b.initialized {
		return fmt.Errorf("backend not initialized")
	}

	b.mu.Lock()
	b.batchBuffer = append(b.batchBuffer, data)
	shouldFlush := len(b.batchBuffer) >= b.batchConfig.BatchSize
	b.mu.Unlock()

	if shouldFlush {
		return b.Flush()
	}
	return nil
}

// Flush 将缓冲区的数据写入文件
func (b *FileBackend) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.batchBuffer) == 0 {
		return nil
	}

	// 保存当前位置用于错误恢复
	currentPos := b.filePos

	// 写入批量数据
	for i, item := range b.batchBuffer {
		// 处理逗号分隔
		if b.needsComma || i > 0 {
			if _, err := b.fileWriter.WriteString(",\n"); err != nil {
				b.recoverFromError(currentPos)
				return fmt.Errorf("failed to write separator: %w", err)
			}
			b.filePos += 2
		} else {
			b.needsComma = true
		}

		if err := b.fileEncoder.Encode(item); err != nil {
			b.recoverFromError(currentPos)
			return fmt.Errorf("failed to encode data: %w", err)
		}

		// 更新位置
		if err := b.fileWriter.Flush(); err != nil {
			b.recoverFromError(currentPos)
			return fmt.Errorf("failed to flush during position update: %w", err)
		}

		newPos, err := b.fileHandle.Seek(0, io.SeekCurrent)
		if err != nil {
			b.recoverFromError(currentPos)
			return fmt.Errorf("failed to get new file position: %w", err)
		}

		b.filePos = newPos
		b.writeCounter++
	}

	// 清空缓冲区
	b.batchBuffer = b.batchBuffer[:0]

	// 刷新写入器
	if err := b.fileWriter.Flush(); err != nil {
		b.recoverFromError(currentPos)
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	return nil
}

func (b *FileBackend) validateEncodedJSON(start, end int64) error {
	if _, err := b.fileHandle.Seek(start, io.SeekStart); err != nil {
		return err
	}

	length := end - start
	data := make([]byte, length)
	if _, err := io.ReadFull(b.fileHandle, data); err != nil {
		return err
	}

	var dummy interface{}
	if err := json.Unmarshal(data, &dummy); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	return nil
}

func (b *FileBackend) recoverFromError(pos int64) {
	b.fileWriter.Flush()
	b.fileHandle.Truncate(pos)
	b.fileHandle.Seek(pos, io.SeekStart)
	b.filePos = pos
}

func (b *FileBackend) validateAndPrepareExistingFile() error {
	if b.originalSize < 2 {
		return errors.New("file too small to be valid JSON array")
	}
	_, err := b.fileHandle.Seek(-2, io.SeekEnd)
	if err != nil {
		return err
	}

	lastTwoBytes := make([]byte, 2)
	_, err = io.ReadFull(b.fileHandle, lastTwoBytes)
	if err != nil {
		return err
	}

	if lastTwoBytes[1] != ']' {
		return errors.New("file does not end with JSON array terminator")
	}

	if lastTwoBytes[0] == '[' {
		_, err = b.fileHandle.Seek(-1, io.SeekEnd)
		b.needsComma = false
	} else {
		_, err = b.fileHandle.Seek(-1, io.SeekEnd)
		b.needsComma = true
	}

	if err != nil {
		return err
	}

	if _, err := b.fileWriter.WriteString("\n"); err != nil {
		return err
	}

	return nil
}

func (b *FileBackend) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.initialized {
		return nil
	}

	// 停止定时刷新
	if b.flushTicker != nil {
		b.flushTicker.Stop()
		close(b.flushStopChan)
	}

	// 处理剩余数据
	if len(b.batchBuffer) > 0 {
		if err := b.writeBatch(b.batchBuffer); err != nil {
			return err
		}
	}

	// 检查是否是空数组情况
	if b.writeCounter == 0 {
		if _, err := b.fileHandle.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek: %w", err)
		}
		if _, err := b.fileWriter.WriteString("[\n]"); err != nil {
			return fmt.Errorf("failed to maintain empty array: %w", err)
		}
	} else {
		if _, err := b.fileWriter.WriteString("\n]"); err != nil {
			return fmt.Errorf("failed to write array end: %w", err)
		}
	}

	if err := b.fileWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if err := b.fileHandle.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	b.initialized = false
	return nil
}

// writeBatch 内部批量写入方法，不处理缓冲区状态
func (b *FileBackend) writeBatch(batch []v1.Data) error {
	if len(batch) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// 保存当前位置用于错误恢复
	currentPos := b.filePos

	// 写入数组开始标记（如果是第一次写入）
	if b.writeCounter == 0 && b.originalSize == 0 {
		if _, err := b.fileWriter.WriteString("[\n"); err != nil {
			return fmt.Errorf("failed to write array start: %w", err)
		}
		b.filePos += 2
	}

	// 写入批量数据
	for i, item := range batch {
		// 处理逗号分隔
		if b.needsComma || i > 0 {
			if _, err := b.fileWriter.WriteString(",\n"); err != nil {
				b.recoverFromError(currentPos)
				return fmt.Errorf("failed to write separator: %w", err)
			}
			b.filePos += 2
		}

		// 记录编码前位置
		startPos := b.filePos

		if err := b.fileEncoder.Encode(item); err != nil {
			b.recoverFromError(currentPos)
			return fmt.Errorf("failed to encode data: %w", err)
		}

		// 更新位置
		if err := b.fileWriter.Flush(); err != nil {
			b.recoverFromError(currentPos)
			return fmt.Errorf("failed to flush during position update: %w", err)
		}

		newPos, err := b.fileHandle.Seek(0, io.SeekCurrent)
		if err != nil {
			b.recoverFromError(currentPos)
			return fmt.Errorf("failed to get new file position: %w", err)
		}

		b.filePos = newPos
		b.writeCounter++
		b.needsComma = true

		// 检查编码后的JSON是否有效
		if err := b.validateEncodedJSON(startPos, b.filePos); err != nil {
			b.recoverFromError(currentPos)
			return fmt.Errorf("invalid JSON generated: %w", err)
		}
	}

	// 立即刷新而不是等待缓冲区满
	if err := b.fileWriter.Flush(); err != nil {
		b.recoverFromError(currentPos)
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	return nil
}
