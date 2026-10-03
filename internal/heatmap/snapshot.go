package heatmap

// Snapshot 某一时刻已提交的完整只读视图。
// 查询只作用于 Snapshot，聚合推进不影响已发布的快照。
type Snapshot struct {
	cells     []Cell
	revisions []Revision
	watermark int64 // unix nano
}

// Cells 返回视图内全部单元（副本）。
func (s *Snapshot) Cells() []Cell {
	out := make([]Cell, len(s.cells))
	copy(out, s.cells)
	return out
}

// Revisions 返回视图内全部修订记录（副本）。
func (s *Snapshot) Revisions() []Revision {
	out := make([]Revision, len(s.revisions))
	copy(out, s.revisions)
	return out
}

// Watermark 返回视图对应的水位时间（unix nano）。
func (s *Snapshot) Watermark() int64 {
	return s.watermark
}
