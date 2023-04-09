package dataset

import (
	//"fmt"
	"fmt"
	"testing"
	"time"
)

type (
	abc struct {
		Name string
		Time time.Time
		Age  int `field:"age"`
	}
)

var ds = NewDataSet(
	WithData(
		map[string]any{
			"id":   nil,
			"name": "dataset1",
		},
		map[string]any{
			"id":   0,
			"name": "dataset1",
		},
		map[string]any{
			"id":   1,
			"name": "dataset1",
		},
		map[string]any{
			"id":   2,
			"name": "dataset2",
		},
		map[string]any{
			"id":   3,
			"name": "dataset3",
		}),
)

func TestDatasetKeys(t *testing.T) {
	fmt.Println(ds.Keys("id")...)
}

func TestDatasetGroupby(t *testing.T) {
	result := ds.GroupBy("name")
	fmt.Println(result)
}

func TestDatasetToStruct(t *testing.T) {
	rec := NewRecordSet()
	// 测试动态添加字段和值
	rec.FieldByName("name").AsString("abc")
	rec.FieldByName("age").AsInterface(10)
	rec.FieldByName("time").AsInterface(time.Now())

	//son := &abc{}
	var son abc
	rec.AsStruct(&son)
	fmt.Println(son)
}

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
