package dataset

import (
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

func TestDatasetFieldByIndex(t *testing.T) {
	fieldset := ds.Record().FieldByIndex(3)
	fmt.Println(fieldset.AsBoolean())
}

func TestDatasetSetByField(t *testing.T) {
	ds.Record().SetByField("new", "gggg")
}

func TestDatasetAppendRecord(t *testing.T) {
	ds := NewDataSet()

	rec := NewRecordSet()
	// 测试动态添加字段和值
	rec.SetByField("name", "abc")
	rec.SetByField("age", 10)
	rec.SetByField("time", time.Now())
	err := ds.AppendRecord(rec)
	if err != nil {
		t.Fatal(err)
	}

	rec2 := NewRecordSet()
	// 测试动态添加字段和值
	rec2.SetByField("name2", "abc2")
	rec2.SetByField("age2", 12)
	rec2.SetByField("time2", time.Now())
	err = ds.AppendRecord(rec2)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ds.Data[0].GetByField("name"))
	fmt.Println(ds.Data[1].GetByField("name"))
	fmt.Println(ds.Data[1].GetByField("name2"))

	if ds.Count() != 2 {
		t.Fatalf("AppendRecord fail")
	}
}

func TestDatasetToStruct(t *testing.T) {
	rec := NewRecordSet()
	// 测试动态添加字段和值
	rec.SetByField("name", "abc")
	rec.SetByField("age", 10)
	rec.SetByField("time", time.Now())

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
	ds.Record().SetByField("name", "dataset")
	t.Log(ds.Count(), ds.Position, ds.Record().AsMap())
	ds.Next()
	ds.Record().SetByField("key", "dataset")
	t.Log(ds.Count(), ds.Position, ds.Record().AsMap())

	rec := NewRecordSet()
	// 测试动态添加字段和值
	rec.SetByField("name", "AAAA")
	rec.SetByField("name2", map[string]string{"fasd": "asdf"})

	ds.AppendRecord(rec)
	t.Log("jjjj")
	ds.classic = false
	ds.Record().SetByField("name3", "CCCC")
	ds.Record().SetByField("sdfsdfd", "CCCC")

	t.Log(rec.AsMap(), ds.Count(), ds.Record().AsMap())
	t.Log(ds.FieldByName("name").AsString())
	t.Log(ds.FieldByName("name2").AsInterface(), ds.Record().Fields(), ds.Record().FieldByName("name2"))
	t.Log(ds.FieldByName("name3").AsString(), ds.Record().FieldByName("name3"))

	ds.Delete()
	t.Log(rec.AsMap(), ds.Count(), ds.Record().AsMap())

}
