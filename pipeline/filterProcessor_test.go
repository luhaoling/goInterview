package pipeline

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	v1 "goInterview/config"
	"io"
	"log"
	"os"
	"sync"
	"testing"
)

func TestFilterProcessor_Process(t *testing.T) {
	InitBatchStore("output.json")
	type fields struct {
		config FilterConfig
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
				config: FilterConfig{
					Field:  "env",
					Values: []string{"prod", "dev"},
				},
			},
			args: args{
				in: Start(),
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &FilterProcessor{
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

func Start() <-chan v1.Data {
	wg := sync.WaitGroup{}
	dataChan := make(chan v1.Data, 100)
	var path []string
	for i := 1; i < 2; i++ {
		path = append(path, fmt.Sprintf("../../%v/input%d.json", "input", i))
	}
	wg.Add(len(path))

	for _, p := range path {
		go func() {
			defer wg.Done()
			file, err := os.Open(p)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			decoder := json.NewDecoder(file)

			if _, err := decoder.Token(); err != nil {
				log.Fatal("Error get json first tag:", err)
			}

			for decoder.More() {
				var data v1.Data
				if err := decoder.Decode(&data); err != nil {
					log.Printf("Error decoding JSON: %v", err)
					close(dataChan)
					return
				}
				dataChan <- data
			}

			if _, err := decoder.Token(); err != nil {
				log.Fatal("Error get json first tag:", err)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(dataChan)
	}()
	return dataChan
}

var (
	fileMutex    sync.Mutex
	fileWriter   *bufio.Writer
	fileEncoder  *json.Encoder
	fileHandle   *os.File
	initialized  bool
	needsComma   bool
	writeCounter int
	filePos      int64
	originalSize int64
)

const (
	batchSize  = 1000
	bufferSize = 65536
)

func InitBatchStore(filename string) error {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	if initialized {
		return nil
	}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	fileHandle = file
	fileWriter = bufio.NewWriterSize(file, bufferSize)
	fileEncoder = json.NewEncoder(fileWriter)
	writeCounter = 0
	needsComma = false

	// 获取文件原始大小和内容
	originalSize, err = fileHandle.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to get file size: %w", err)
	}

	// 处理空文件情况
	if originalSize == 0 {
		// 写入空数组格式 "[ ]"（带空格更美观）
		if _, err := fileWriter.WriteString("[ ]"); err != nil {
			file.Close()
			return fmt.Errorf("failed to write empty array: %w", err)
		}

		// 刷新缓冲区确保写入磁盘
		if err := fileWriter.Flush(); err != nil {
			file.Close()
			return fmt.Errorf("failed to flush empty array: %w", err)
		}

		// 重置文件指针到数组开始位置后
		if _, err := fileHandle.Seek(1, io.SeekStart); err != nil {
			file.Close()
			return fmt.Errorf("failed to seek after empty array: %w", err)
		}

		// 标记已有内容（一个空格）
		needsComma = false
	} else {
		// 处理已有内容
		if err := validateAndPrepareExistingFile(); err != nil {
			file.Close()
			return fmt.Errorf("invalid existing content: %w", err)
		}
	}

	// 获取当前写入位置
	filePos, err = fileHandle.Seek(0, io.SeekCurrent)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to get file position: %w", err)
	}

	initialized = true
	return nil
}

func validateAndPrepareExistingFile() error {
	// 读取文件最后几个字节检查格式
	if originalSize < 2 { // 至少要有 "[]"
		return errors.New("file too small to be valid JSON array")
	}

	_, err := fileHandle.Seek(-2, io.SeekEnd)
	if err != nil {
		return err
	}

	lastTwoBytes := make([]byte, 2)
	_, err = io.ReadFull(fileHandle, lastTwoBytes)
	if err != nil {
		return err
	}

	// 检查是否以"]"结尾
	if lastTwoBytes[1] != ']' {
		return errors.New("file does not end with JSON array terminator")
	}

	// 检查是否有内容
	if lastTwoBytes[0] == '[' {
		// 空数组 "[]"
		_, err = fileHandle.Seek(-1, io.SeekEnd) // 移到"["后面
		needsComma = false
	} else {
		// 非空数组 "[...]"
		_, err = fileHandle.Seek(-1, io.SeekEnd) // 移到"]"前面
		needsComma = true
	}

	if err != nil {
		return err
	}

	// 准备追加新数据
	if _, err := fileWriter.WriteString("\n"); err != nil {
		return err
	}

	return nil
}

func CloseBatchStore() error {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	if !initialized {
		return nil
	}

	// 检查是否是空数组情况
	if writeCounter == 0 {
		// 确保保持 [ ] 格式
		if _, err := fileHandle.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek: %w", err)
		}
		if _, err := fileWriter.WriteString("[ ]"); err != nil {
			return fmt.Errorf("failed to maintain empty array: %w", err)
		}
	} else {
		// 正常关闭流程
		if _, err := fileWriter.WriteString("]"); err != nil {
			return fmt.Errorf("failed to write array end: %w", err)
		}
	}

	if err := fileWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if err := fileHandle.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	initialized = false
	return nil
}

func BatchStore(data <-chan v1.Data) error {
	if !initialized {
		return fmt.Errorf("batch store not initialized")
	}

	batch := make([]v1.Data, 0, batchSize)

	for item := range data {
		batch = append(batch, item)

		if len(batch) >= batchSize {
			if err := writeBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return writeBatch(batch)
	}

	return nil
}

func writeBatch(batch []v1.Data) error {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	// 保存当前位置用于错误恢复
	currentPos := filePos

	for _, item := range batch {
		// 处理逗号分隔
		if needsComma {
			if _, err := fileWriter.WriteString(",\n"); err != nil {
				recoverFromError(currentPos)
				return fmt.Errorf("failed to write separator: %w", err)
			}
			filePos += 2 // 逗号和换行符
		} else {
			needsComma = true
		}

		// 记录编码前位置
		startPos := filePos

		if err := fileEncoder.Encode(item); err != nil {
			recoverFromError(currentPos)
			return fmt.Errorf("failed to encode data: %w", err)
		}

		// 更新位置
		if err := fileWriter.Flush(); err != nil {
			recoverFromError(currentPos)
			return fmt.Errorf("failed to flush during position update: %w", err)
		}

		newPos, err := fileHandle.Seek(0, io.SeekCurrent)
		if err != nil {
			recoverFromError(currentPos)
			return fmt.Errorf("failed to get new file position: %w", err)
		}

		filePos = newPos
		writeCounter++

		// 检查编码后的JSON是否有效
		if err := validateEncodedJSON(startPos, filePos); err != nil {
			recoverFromError(currentPos)
			return fmt.Errorf("invalid JSON generated: %w", err)
		}
	}

	// 定期刷新而不是每次写入都刷新
	if writeCounter%batchSize == 0 {
		if err := fileWriter.Flush(); err != nil {
			recoverFromError(currentPos)
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
	}

	return nil
}

func validateEncodedJSON(start, end int64) error {
	if _, err := fileHandle.Seek(start, io.SeekStart); err != nil {
		return err
	}

	length := end - start
	data := make([]byte, length)
	if _, err := io.ReadFull(fileHandle, data); err != nil {
		return err
	}

	// 检查是否是有效的JSON
	var dummy interface{}
	if err := json.Unmarshal(data, &dummy); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	return nil
}

func recoverFromError(pos int64) {
	fileWriter.Flush()
	fileHandle.Truncate(pos)
	fileHandle.Seek(pos, io.SeekStart)
	filePos = pos
}
