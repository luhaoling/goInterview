package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"goInterview/config"
	"io"
	"net/http"
	"sync"
	"time"
)

// 从网络中拉取数据
const (
	TIMEOUT time.Duration = 30
)

type HTTPReceiver struct {
	httpPath []string
	dataChan chan config.Data // 传输解析后的数据
	done     chan struct{}
	client   *http.Client
	timeout  time.Duration
}

func (h *HTTPReceiver) Receive() <-chan config.Data {
	return h.dataChan
}

func (h *HTTPReceiver) Stop() error {
	close(h.done)
	return nil
}

func NewHTTPReceiver(conf config.Receiver) Receiver {
	return &HTTPReceiver{
		httpPath: conf.Path,
		dataChan: make(chan config.Data, conf.ChanSize),
		done:     make(chan struct{}),
		client: &http.Client{
			Timeout: TIMEOUT,
		},
		timeout: TIMEOUT,
	}
}

func (h *HTTPReceiver) Start() error {
	wg := sync.WaitGroup{}
	wg.Add(len(h.httpPath))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, url := range h.httpPath {
		u := url // 创建局部变量副本
		go func() {
			defer wg.Done()
			h.fetchAndParseData(ctx, u)
		}()
	}

	go func() {
		wg.Wait()
		close(h.dataChan)
	}()

	return nil
}

func (h *HTTPReceiver) fetchAndParseData(ctx context.Context, url string) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		fmt.Printf("Error creating request for %s: %v\n", url, err)
		return
	}

	resp, err := h.client.Do(req)
	if err != nil {
		fmt.Printf("Error fetching %s: %v\n", url, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Unexpected status from %s: %s\n", url, resp.Status)
		return
	}

	// 读取整个响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response from %s: %v\n", url, err)
		return
	}

	// 解析JSON数组
	var jsonData []map[string]interface{}
	if err := json.Unmarshal(body, &jsonData); err != nil {
		fmt.Printf("Error parsing JSON from %s: %v\n", url, err)
		return
	}

	// 处理每条数据
	for _, item := range jsonData {
		select {
		case <-h.done:
			return
		case <-ctx.Done():
			return
		default:
			// 添加来源信息
			data := make(config.Data)
			for k, v := range item {
				data[k] = v
			}

			// 发送到处理通道
			select {
			case h.dataChan <- data:
			case <-h.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}
