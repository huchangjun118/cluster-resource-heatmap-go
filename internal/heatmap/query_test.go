package heatmap

import (
	"testing"
	"time"
)

// 构造包含两个池、三个节点、两个窗口的已封口快照。
func buildSnapshot(t *testing.T) *Snapshot {
	t.Helper()
	cfg := testConfig()
	a := newAggregator(cfg)
	samples := []Sample{
		{SampleID: "1", NodeID: "n1", Pool: "p1", Resource: ResourceCPU, Value: 1, Timestamp: ts(1)},
		{SampleID: "2", NodeID: "n1", Pool: "p1", Resource: ResourceCPU, Value: 2, Timestamp: ts(2)},
		{SampleID: "3", NodeID: "n2", Pool: "p1", Resource: ResourceCPU, Value: 4, Timestamp: ts(3)},
		{SampleID: "4", NodeID: "n3", Pool: "p2", Resource: ResourceCPU, Value: 8, Timestamp: ts(4)},
		{SampleID: "5", NodeID: "n1", Pool: "p1", Resource: ResourceMemory, Value: 16, Timestamp: ts(5)},
		// 第二个窗口 [60,120)
		{SampleID: "6", NodeID: "n1", Pool: "p1", Resource: ResourceCPU, Value: 32, Timestamp: ts(61)},
		// 推进水位封口两个窗口
		{SampleID: "7", NodeID: "n1", Pool: "p1", Resource: ResourceCPU, Value: 64, Timestamp: ts(200)},
	}
	for _, s := range samples {
		a.ingest(s)
	}
	return a.snapshot()
}

func TestQueryNodeLevel(t *testing.T) {
	snap := buildSnapshot(t)
	rows := QuerySnapshot(snap, Query{NodeID: "n1", Resource: ResourceCPU}, GroupByNode)
	if len(rows) != 3 { // 窗口[0,60)、[60,120)、[180,240)
		t.Fatalf("rows: %+v", rows)
	}
	if rows[0].Count != 2 || rows[0].Sum != 3 || rows[0].Min != 1 || rows[0].Max != 2 {
		t.Fatalf("row0: %+v", rows[0])
	}
}

// 池级与集群级必须由节点级派生且自洽。
func TestQueryHierarchyConsistency(t *testing.T) {
	snap := buildSnapshot(t)
	q := Query{Resource: ResourceCPU, From: ts(0), To: ts(120)}

	nodeRows := QuerySnapshot(snap, q, GroupByNode)
	poolRows := QuerySnapshot(snap, q, GroupByPool)
	clusterRows := QuerySnapshot(snap, q, GroupByCluster)

	// 集群级 == 池级之和 == 节点级之和
	var nodeCount, poolCount int64
	var nodeSum, poolSum float64
	for _, r := range nodeRows {
		nodeCount += r.Count
		nodeSum += r.Sum
	}
	for _, r := range poolRows {
		poolCount += r.Count
		poolSum += r.Sum
	}
	if len(clusterRows) != 2 {
		t.Fatalf("cluster rows: %+v", clusterRows)
	}
	var clusterCount int64
	var clusterSum float64
	for _, r := range clusterRows {
		clusterCount += r.Count
		clusterSum += r.Sum
	}
	if nodeCount != poolCount || nodeCount != clusterCount || nodeSum != poolSum || nodeSum != clusterSum {
		t.Fatalf("inconsistent: node(%d,%v) pool(%d,%v) cluster(%d,%v)",
			nodeCount, nodeSum, poolCount, poolSum, clusterCount, clusterSum)
	}
	if clusterCount != 5 || clusterSum != 47 {
		t.Fatalf("cluster total: %d %v", clusterCount, clusterSum)
	}

	// 池级过滤
	p1 := QuerySnapshot(snap, Query{Pool: "p1", Resource: ResourceCPU, From: ts(0), To: ts(60)}, GroupByPool)
	if len(p1) != 1 || p1[0].Group != "p1" || p1[0].Count != 3 || p1[0].Sum != 7 {
		t.Fatalf("pool p1: %+v", p1)
	}
}

// 时间范围过滤按窗口起点，不重复不漏计。
func TestQueryTimeRange(t *testing.T) {
	snap := buildSnapshot(t)
	all := QuerySnapshot(snap, Query{Resource: ResourceCPU}, GroupByCluster)
	ranged := QuerySnapshot(snap, Query{Resource: ResourceCPU, From: ts(0), To: ts(120)}, GroupByCluster)
	if len(all) != 3 || len(ranged) != 2 {
		t.Fatalf("all=%d ranged=%d", len(all), len(ranged))
	}
	var allCount, rangedCount int64
	for _, r := range all {
		allCount += r.Count
	}
	for _, r := range ranged {
		rangedCount += r.Count
	}
	if rangedCount >= allCount {
		t.Fatalf("range filter ineffective: %d vs %d", rangedCount, allCount)
	}
}

// 空快照查询返回空。
func TestQueryEmpty(t *testing.T) {
	snap := newAggregator(testConfig()).snapshot()
	if rows := QuerySnapshot(snap, Query{}, GroupByCluster); len(rows) != 0 {
		t.Fatalf("want empty, got %+v", rows)
	}
	_ = time.Now
}
