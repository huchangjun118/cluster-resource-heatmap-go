// Package heatmap 提供集群资源热力图数据服务：
// 将逐节点资源采样按时间窗口聚合成可查询的热力图单元。
package heatmap

import "time"

// ResourceType 资源类型。
type ResourceType string

const (
	ResourceCPU    ResourceType = "cpu"
	ResourceMemory ResourceType = "memory"
	ResourceDiskIO ResourceType = "disk_io"
	ResourceNetIO  ResourceType = "net_io"
)

// Sample 单节点单次资源采样。
type Sample struct {
	SampleID  string       // 全局唯一，用于去重
	NodeID    string       // 节点标识
	Pool      string       // 节点池
	Resource  ResourceType // 资源类型
	Value     float64      // 采样值
	Timestamp time.Time    // 事件时间（采样发生时间）
}

// Window 时间窗口，[Start, End) 半开区间。
type Window struct {
	Start time.Time
	End   time.Time
}

// Cell 热力图单元：一个窗口内某节点某资源的聚合结果。
type Cell struct {
	Window   Window
	NodeID   string
	Pool     string
	Resource ResourceType

	Count int64
	Sum   float64
	Min   float64
	Max   float64

	Version int  // 修订版本，初始封口为 1，每次补算 +1
	Sealed  bool // 是否已封口
}

// Avg 平均值。
func (c Cell) Avg() float64 {
	if c.Count == 0 {
		return 0
	}
	return c.Sum / float64(c.Count)
}

// Revision 一次窗口修订记录。
type Revision struct {
	Window    Window
	NodeID    string
	Resource  ResourceType
	Version   int
	Reason    string
	RevisedAt time.Time
}

// Config 服务配置。
type Config struct {
	WindowSize      time.Duration // 窗口长度
	SlideStep       time.Duration // 滑动步长
	AllowedLateness time.Duration // 水位允许的迟到时长
	RevisionHorizon time.Duration // 封口后仍接受补算的时长
	QueueCapacity   int           // 摄入队列容量（背压上界）
	DataDir         string        // 持久化目录，空则纯内存
	SnapshotEvery   time.Duration // 快照发布周期
}
