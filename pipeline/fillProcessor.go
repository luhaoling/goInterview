package pipeline

import (
	"fmt"
	v1 "goInterview/config"
	"os/exec"
	"runtime"
	"syscall"
	"unsafe"
)

// FillProcessor 填充数据
type FillProcessor struct {
	config FillConfig
}

type FillConfig struct {
	Fields []string // 字段名到获取方式的映射
	Open   bool
}

func NewFillProcessor(config FillConfig) *FillProcessor {
	return &FillProcessor{config: config}
}

func (p *FillProcessor) Process(in <-chan v1.Data) (<-chan v1.Data, error) {
	if !p.config.Open {
		return in, nil
	}
	out := make(chan v1.Data, 100)

	go func() {
		defer close(out)
		for data := range in {

			for _, field := range p.config.Fields {
				switch field {
				case "os_type":
					data[field] = runtime.GOOS
				case "os_version":
					version, _ := getOSVersion()
					data[field] = version
				default:
					data[field] = "unknow" // 直接使用配置值
				}
			}
			out <- data
		}
	}()

	return out, nil
}

func getOSVersion() (string, error) {
	switch runtime.GOOS {
	case "linux":
		out, err := exec.Command("uname", "-r").Output()
		if err != nil {
			return "", err
		}
		return string(out), nil
	case "windows":
		out, err := getWindowsVersion()
		if err != nil {
			return "", err
		}
		return out, nil
	default:
		return "unknown", nil
	}
}

func getWindowsVersion() (string, error) {
	// 加载 kernel32.dll
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	// 获取 GetVersionExW 函数
	getVersionEx := kernel32.NewProc("GetVersionExW")

	type OSVersionInfoEx struct {
		OSVersionInfoSize uint32
		MajorVersion      uint32
		MinorVersion      uint32
		BuildNumber       uint32
		PlatformID        uint32
		CSDVersion        [128]uint16
		ServicePackMajor  uint16
		ServicePackMinor  uint16
		SuiteMask         uint16
		ProductType       byte
		Reserved          byte
	}

	var info OSVersionInfoEx
	info.OSVersionInfoSize = uint32(unsafe.Sizeof(info))

	ret, _, _ := getVersionEx.Call(uintptr(unsafe.Pointer(&info)))
	if ret == 0 {
		return "", fmt.Errorf("GetVersionEx failed")
	}

	version := fmt.Sprintf("%d.%d.%d", info.MajorVersion, info.MinorVersion, info.BuildNumber)
	return version, nil
}
