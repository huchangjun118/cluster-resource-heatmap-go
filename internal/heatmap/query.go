package heatmap

import (
	"sort"
	"time"
)

// Query 下钻查询条件。零值字段表示不过滤。
// 时间范围按窗口起点过滤：窗口 Start ∈ [From, To)。
type Query struct {
	NodeID   string       // 精确节点，空为全部
	Pool     string       // 节点池，空为全部
	Resource ResourceType // 资源类型，空为全部
	From     time.Time    // 窗口起点下界（含），零值为不限
	To       time.Time    // 窗口起点上界（不含），零值为不限
}

// GroupBy 聚合层级。
type GroupBy int

const (
	GroupByNode    GroupBy = iota // 节点级（最细）
	GroupByPool                   // 节点池级
	GroupByCluster                // 集群级（最粗）
)

// Row 查询结果行：一个分组在一个窗口上的聚合。
type Row struct {
	Window   Window
	Group    string // 节点 ID / 池名 / "cluster"
	Resource ResourceType
	Count    int64
	Sum      float64
	Min      float64
	Max      float64
}

// Avg 平均值。
func (r Row) Avg() float64 {
	if r.Count == 0 {
		return 0
	}
	return r.Sum / float64(r.Count)
}

// QuerySnapshot 在快照上执行下钻查询。
// 粗粒度行由细粒度单元直接合并派生：Count/Sum 相加、Min 取小、Max 取大，
// 因此任一层级结果与底层单元自洽，不重复计数也不漏计。
func QuerySnapshot(s *Snapshot, q Query, groupBy GroupBy) []Row {
	type rowKey struct {
		windowStart time.Time
		group       string
		resource    ResourceType
	}
	merged := make(map[rowKey]*Row)
	var order []rowKey

	for _, c := range s.cells {
		if !matchCell(c, q) {
			continue
		}
		group := c.NodeID
		switch groupBy {
		case GroupByPool:
			group = c.Pool
		case GroupByCluster:
			group = "cluster"
		}
		k := rowKey{windowStart: c.Window.Start, group: group, resource: c.Resource}
		r := merged[k]
		if r == nil {
			r = &Row{Window: c.Window, Group: group, Resource: c.Resource}
			merged[k] = r
			order = append(order, k)
		}
		if r.Count == 0 || c.Min < r.Min {
			r.Min = c.Min
		}
		if r.Count == 0 || c.Max > r.Max {
			r.Max = c.Max
		}
		r.Count += c.Count
		r.Sum += c.Sum
	}

	rows := make([]Row, 0, len(merged))
	for _, k := range order {
		rows = append(rows, *merged[k])
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].Window.Start.Equal(rows[j].Window.Start) {
			return rows[i].Window.Start.Before(rows[j].Window.Start)
		}
		if rows[i].Group != rows[j].Group {
			return rows[i].Group < rows[j].Group
		}
		return rows[i].Resource < rows[j].Resource
	})
	return rows
}

// matchCell 判断单元是否命中查询条件。
func matchCell(c Cell, q Query) bool {
	if q.NodeID != "" && c.NodeID != q.NodeID {
		return false
	}
	if q.Pool != "" && c.Pool != q.Pool {
		return false
	}
	if q.Resource != "" && c.Resource != q.Resource {
		return false
	}
	if !q.From.IsZero() && c.Window.Start.Before(q.From) {
		return false
	}
	if !q.To.IsZero() && !c.Window.Start.Before(q.To) {
		return false
	}
	return true
}
