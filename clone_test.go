package dataset

import (
	"strings"
	"testing"
)

func newCloneFixture() *TDataSet {
	ds := NewDataSet()
	ds.Name = "res.partner"
	ds.KeyField = "id"
	ds.NewRecord(map[string]any{"id": int64(1), "name": "alice"})
	ds.NewRecord(map[string]any{"id": int64(2), "name": "bob"})
	ds.SetFieldFormater("name", func(v any) any { return strings.ToUpper(v.(string)) })
	ds.Classic(true)
	ds.First()
	return ds
}

func TestCloneCopiesScalarState(t *testing.T) {
	src := newCloneFixture()
	dst := src.Clone()

	if dst == nil {
		t.Fatal("Clone 返回 nil")
	}
	if dst == src {
		t.Fatal("Clone 返回了同一个指针")
	}
	if dst.Name != "res.partner" || dst.KeyField != "id" {
		t.Errorf("Name/KeyField 未复制: %q %q", dst.Name, dst.KeyField)
	}
	if !dst.Classic() {
		t.Error("classic 标志未复制")
	}
	if dst.Count() != 2 {
		t.Fatalf("记录数 %d, want 2", dst.Count())
	}
	if dst.FieldCount != src.FieldCount {
		t.Errorf("FieldCount %d, want %d", dst.FieldCount, src.FieldCount)
	}
}

// 格式化器必须跟着走：orm 的 _scanRows 靠它做 int64→string 等输出转换，
// 副本丢了格式化器就等于换了一套输出语义。
func TestCloneCarriesFieldFormater(t *testing.T) {
	src := newCloneFixture()
	dst := src.Clone()

	dst.First()
	if got := dst.Record().AsMap()["name"]; got != "ALICE" {
		t.Fatalf("副本未套用格式化器: got %v, want ALICE", got)
	}

	// 但 formatter map 本身要独立：在副本上加格式化器不能影响原件。
	dst.SetFieldFormater("id", func(v any) any { return "X" })
	src.First()
	if got := src.Record().AsMap()["id"]; got == "X" {
		t.Error("在副本上 SetFieldFormater 污染了原数据集")
	}
}

// 这是 Clone 存在的核心理由：改副本不能影响原件。
func TestCloneRecordValuesAreIndependent(t *testing.T) {
	src := newCloneFixture()
	dst := src.Clone()

	dst.First()
	if !dst.Record().SetByField("name", "changed") {
		t.Fatal("SetByField 失败")
	}

	src.First()
	if got := src.Record().GetByField("name"); got != "alice" {
		t.Fatalf("改副本污染了原数据集: src name = %v, want alice", got)
	}
	dst.First()
	if got := dst.Record().GetByField("name"); got != "changed" {
		t.Fatalf("副本自身未生效: %v", got)
	}
}

// 游标独立：orm 的读路径会对结果集调 First()/Next() 遍历。
func TestCloneCursorIsIndependent(t *testing.T) {
	src := newCloneFixture()
	src.First()
	dst := src.Clone()

	dst.Next()
	dst.Next()
	if !dst.Eof() {
		t.Fatal("副本游标未推进到 Eof")
	}
	if src.Eof() {
		t.Fatal("推进副本游标影响了原数据集")
	}
	if got := src.Record().GetByField("name"); got != "alice" {
		t.Fatalf("原数据集游标被移动: %v", got)
	}
}

// 新增字段只能落在副本上：SetByField 遇到未知字段会 AddField 改字段表。
func TestCloneFieldTableIsIndependent(t *testing.T) {
	src := newCloneFixture()
	dst := src.Clone()

	dst.First()
	dst.Record().SetByField("extra", "v")

	if src.HasField("extra") {
		t.Error("在副本上新增字段污染了原数据集的字段表")
	}
	if !dst.HasField("extra") {
		t.Error("副本自身未新增字段")
	}
}

// RecordsIndex 不能照搬——它的值是原记录的指针。
func TestCloneRebuildsRecordsIndex(t *testing.T) {
	src := newCloneFixture()
	src.SetKeyField("id") // 建立索引
	dst := src.Clone()

	rec := dst.RecordByKey(int64(2))
	if rec == nil {
		t.Fatal("副本上按 key 查不到记录")
	}
	// 必须是副本自己的记录，不能是原数据集的那条。
	for _, srcRec := range src.Data {
		if rec == srcRec {
			t.Fatal("副本的 RecordsIndex 指向了原数据集的记录")
		}
	}
	if got := rec.GetByField("name"); got != "bob" {
		t.Fatalf("按 key 取到的记录不对: %v", got)
	}
}

func TestCloneNilReceiver(t *testing.T) {
	var ds *TDataSet
	if got := ds.Clone(); got != nil {
		t.Fatalf("nil.Clone() = %v, want nil", got)
	}
	var rec *TRecordSet
	if got := rec.Clone(); got != nil {
		t.Fatalf("nil record Clone() = %v, want nil", got)
	}
}

// 脱离数据集的记录副本必须自带字段索引，否则 getFieldsIndex 回退到 nil dataset，
// 所有字段都查不到。
func TestRecordCloneIsDetachedButUsable(t *testing.T) {
	src := newCloneFixture()
	src.First()
	rec := src.Record().Clone()

	if got := rec.GetByField("name"); got != "alice" {
		t.Fatalf("脱离记录取值失败: %v", got)
	}

	rec.SetByField("name", "solo")
	src.First()
	if got := src.Record().GetByField("name"); got != "alice" {
		t.Fatalf("改脱离记录污染了原数据集: %v", got)
	}
	if src.Count() != 2 {
		t.Fatalf("脱离记录被追加回了原数据集: count=%d", src.Count())
	}
}
