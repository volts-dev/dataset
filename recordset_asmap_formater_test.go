package dataset

import (
	"testing"

	"github.com/volts-dev/utils"
)

// TestAsMapSkipsFormaterOnCompositeValues 回归:字段格式化器是**标量**转换器
// (典型来源:BigNumberToString 给雪花 id/外键装的 int64→字符串转换)。经典/嵌套读取时,
// 关系字段的 OnRead 会把标量外键替换成子记录:
//
//	many2one    -> map[string]any
//	one2many    -> []any / []map[string]any
//	many2many   -> []map[string]any
//
// AsMap 若对这些复合值仍套用标量格式化器,map 会被 ToString 成空串、切片被压坏,内嵌数据
// 整个丢失(真实症状:form 里 m2o 字段读出来是空)。此测试确保 AsMap 只对标量套格式化器。
func TestAsMapSkipsFormaterOnCompositeValues(t *testing.T) {
	ds := NewDataSet(WithData(map[string]any{
		"id":         int64(1),
		"partner_id": map[string]any{"id": int64(9), "name": "ACME"}, // m2o 内嵌子记录
		"line_ids":   []any{int64(2), int64(3)},                      // o2m 内嵌 id 列表
		"tag_ids":    []map[string]any{{"id": int64(5), "name": "vip"}}, // m2m 内嵌子记录
	}))

	// 模拟 BigNumberToString:给每个 id/外键列装一个"int64→字符串"的标量格式化器。
	strFormater := func(v any) any { return utils.ToString(v) }
	for _, f := range []string{"id", "partner_id", "line_ids", "tag_ids"} {
		ds.SetFieldFormater(f, strFormater)
	}

	m := ds.Record().AsMap()

	// 标量列:格式化器正常生效(int64(1) -> "1")。
	if got := m["id"]; got != "1" {
		t.Fatalf("标量 id 期望格式化为 \"1\", 实际 %T=%v", got, got)
	}

	// m2o:内嵌 map 必须原样保留,不能被标量格式化器压成空串/字符串。
	pm, ok := m["partner_id"].(map[string]any)
	if !ok {
		t.Fatalf("partner_id 期望保留 map, 实际 %T=%v", m["partner_id"], m["partner_id"])
	}
	if pm["name"] != "ACME" {
		t.Fatalf("partner_id.name 期望 ACME, 实际 %v", pm["name"])
	}

	// o2m:内嵌 []any 必须原样保留。
	if lines, ok := m["line_ids"].([]any); !ok || len(lines) != 2 {
		t.Fatalf("line_ids 期望保留 []any(len 2), 实际 %T=%v", m["line_ids"], m["line_ids"])
	}

	// m2m:内嵌 []map 必须原样保留。
	if tags, ok := m["tag_ids"].([]map[string]any); !ok || len(tags) != 1 {
		t.Fatalf("tag_ids 期望保留 []map(len 1), 实际 %T=%v", m["tag_ids"], m["tag_ids"])
	}
}
