package dataset

import (
	//"fmt"
	"testing"
)

func TestDataset_NewRec(t *testing.T) {
	ds := NewDataSet()
	rec := NewRecordSet()
	// 测试动态添加字段和值
	rec.FieldByName("name").AsString("AAAA")
	rec.FieldByName("name2").AsInterface(map[string]string{"fasd": "asdf"})
	ds.AppendRecord(rec)
	ds.classic = false
	ds.FieldByName("name3").AsString("CCCC")

	t.Log(rec.AsItfMap(), ds.Count(), ds.Record().AsItfMap())
	t.Log(ds.FieldByName("name").AsString())
	t.Log(ds.FieldByName("name2").AsInterface(), ds.Record().Fields(), ds.Record().FieldByName("name2"))
	t.Log(ds.FieldByName("name3").AsString(), ds.Record().FieldByName("name3"))

	ds.Delete()
	t.Log(rec.AsItfMap(), ds.Count(), ds.Record().AsItfMap())

}
