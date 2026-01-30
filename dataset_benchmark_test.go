package dataset

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func makeDataset(n int) *TDataSet {
	ds := NewDataSet()
	for i := 0; i < n; i++ {
		rec := NewRecordSet()
		rec.SetByField("id", i)
		rec.SetByField("name", fmt.Sprintf("name-%d", i))
		rec.SetByField("value", i*10)
		ds.AppendRecord(rec)
	}
	return ds
}

func BenchmarkCreateDatasetSmall(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = makeDataset(100)
	}
}

func BenchmarkCreateDatasetLarge(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = makeDataset(10000)
	}
}

func BenchmarkAppendRecordSequential(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ds := NewDataSet()
		rec := NewRecordSet()
		rec.SetByField("id", i)
		rec.SetByField("name", "abc")
		ds.AppendRecord(rec)
	}
}

func BenchmarkAppendRecordPreallocated(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ds := NewDataSet()
		ds.Data = make([]*TRecordSet, 0, 1000)
		for j := 0; j < 1000; j++ {
			rec := NewRecordSet()
			rec.SetByField("id", j)
			rec.SetByField("name", "abc")
			ds.AppendRecord(rec)
		}
	}
}

func BenchmarkAppendRecordConcurrent_WithMutex(b *testing.B) {
	b.ReportAllocs()
	ds := NewDataSet()
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		j := 0
		for pb.Next() {
			j++
			rec := NewRecordSet()
			rec.SetByField("id", j)
			rec.SetByField("name", "abc")
			mu.Lock()
			ds.AppendRecord(rec)
			mu.Unlock()
		}
	})
}

func BenchmarkGetByField_Sequential(b *testing.B) {
	ds := makeDataset(10000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := ds.Data[i%len(ds.Data)]
		_ = rec.GetByField("name")
	}
}

func BenchmarkRecordByField_Sequential(b *testing.B) {
	ds := makeDataset(10000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ds.RecordByField("id", i%10000)
	}
}

func BenchmarkSetKeyFieldAndRecordByKey(b *testing.B) {
	ds := makeDataset(10000)
	ds.SetKeyField("id")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ds.RecordByKey(i % 10000)
	}
}

func BenchmarkKeys(b *testing.B) {
	ds := makeDataset(10000)
	ds.SetKeyField("id")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ds.Keys()
	}
}

func BenchmarkGroupBy(b *testing.B) {
	ds := NewDataSet()
	for i := 0; i < 10000; i++ {
		rec := NewRecordSet()
		rec.SetByField("id", i)
		// generate some grouping
		rec.SetByField("grp", strconv.Itoa(i%10))
		rec.SetByField("name", fmt.Sprintf("name-%d", i))
		ds.AppendRecord(rec)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ds.GroupBy("grp")
	}
}

func BenchmarkRandomAccessParallel(b *testing.B) {
	ds := makeDataset(10000)
	ds.SetKeyField("id")
	rand.Seed(time.Now().UnixNano())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := rand.Intn(10000)
			_ = ds.RecordByKey(k)
		}
	})
}
