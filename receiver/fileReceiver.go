package receiver

import (
	"encoding/json"
	"fmt"
	"goInterview/config"
	"log"
	"os"
	"sync"
)

// FileReceiver 从文件中获取数据
type FileReceiver struct {
	filePath     []string
	dataChan     chan config.Data
	dataChanSize int64
	done         chan struct{}
}

func NewFileReceiver(receiver config.Receiver) Receiver {
	return &FileReceiver{
		filePath: receiver.Path,
		dataChan: make(chan config.Data, receiver.ChanSize),
		done:     make(chan struct{}),
	}
}

func (r *FileReceiver) Receive() <-chan config.Data {
	return r.dataChan
}

func (r *FileReceiver) Start() error {
	wg := sync.WaitGroup{}
	wg.Add(len(r.filePath))

	for _, p := range r.filePath {
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
				var data config.Data
				if err := decoder.Decode(&data); err != nil {
					log.Printf("Error decoding JSON: %v", err)
					close(r.dataChan)
					return
				}
				fmt.Println("data", data)
				r.dataChan <- data
			}
			if _, err := decoder.Token(); err != nil {
				log.Fatal("Error get json first tag:", err)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(r.dataChan)
	}()
	return nil
}

func (r *FileReceiver) Stop() error {
	close(r.done)
	return nil
}
