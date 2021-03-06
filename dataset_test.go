package dataset

import (
	//"fmt"
	"testing"
)

func TestDataset_NewRec(t *testing.T) {
	// 测试动态field添加和验校
	ds := NewDataSet()
	ds.SetFields("name", "key")
	ds.Next()
	ds.FieldByName("name").AsInterface("dataset")
	t.Log(ds.Count(), ds.Position, ds.Record().AsItfMap())
	ds.Next()
	ds.FieldByName("key").AsInterface("dataset")
	t.Log(ds.Count(), ds.Position, ds.Record().AsItfMap())

	rec := NewRecordSet()
	// 测试动态添加字段和值
	rec.FieldByName("name").AsString("AAAA")
	rec.FieldByName("name2").AsInterface(map[string]string{"fasd": "asdf"})

	ds.AppendRecord(rec)
	t.Log("jjjj")
	ds.classic = false
	ds.FieldByName("name3").AsString("CCCC")
	ds.FieldByName("sdfsdfd").AsString("CCCC")

	t.Log(rec.AsItfMap(), ds.Count(), ds.Record().AsItfMap())
	t.Log(ds.FieldByName("name").AsString())
	t.Log(ds.FieldByName("name2").AsInterface(), ds.Record().Fields(), ds.Record().FieldByName("name2"))
	t.Log(ds.FieldByName("name3").AsString(), ds.Record().FieldByName("name3"))

	ds.Delete()
	t.Log(rec.AsItfMap(), ds.Count(), ds.Record().AsItfMap())

}
