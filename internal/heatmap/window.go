package heatmap

import "time"

// windowAssigner 负责把事件时间确定性地映射到窗口集合。
// 窗口网格由 size 与 step 决定，起点对齐到 Unix 纪元，
// 因此窗口归属只取决于 (size, step, 事件时间)，与到达顺序无关。
type windowAssigner struct {
	size time.Duration
	step time.Duration
}

func newWindowAssigner(size, step time.Duration) windowAssigner {
	if step <= 0 {
		step = size
	}
	if size <= 0 {
		size = step
	}
	return windowAssigner{size: size, step: step}
}

// assign 返回事件时间 t 所属的全部窗口（滑动窗口下可能多个）。
// 窗口为 [start, start+size) 半开区间，start 是 step 的整数倍。
func (a windowAssigner) assign(t time.Time) []Window {
	latest := a.windowStartFor(t)
	var out []Window
	// 从包含 t 的最晚窗口向前枚举，直到窗口不再覆盖 t。
	for start := latest; !start.IsZero() && start.Add(a.size).After(t); start = start.Add(-a.step) {
		out = append(out, Window{Start: start, End: start.Add(a.size)})
	}
	return out
}

// windowStartFor 计算包含 t 的最晚窗口起点（向下对齐到 step 网格）。
func (a windowAssigner) windowStartFor(t time.Time) time.Time {
	ns := t.UnixNano()
	step := int64(a.step)
	if step <= 0 {
		step = 1
	}
	aligned := ns - ns%step
	if aligned < 0 && ns%step != 0 { // 处理负时间戳的向下取整
		aligned -= step
	}
	return time.Unix(0, aligned).UTC()
}
