package dataset

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewDataSet(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		ds := NewDataSet()
		if ds == nil {
			t.Fatal("NewDataSet returned nil")
		}
		if !ds.IsEmpty() {
			t.Error("New dataset should be empty")
		}
	})

	t.Run("WithData", func(t *testing.T) {
		ds := NewDataSet(WithData(
			map[string]any{"id": 1, "name": "test1"},
			map[string]any{"id": 2, "name": "test2"},
		))
		if ds.Count() != 2 {
			t.Errorf("Expected 2 records, got %d", ds.Count())
		}
	})

	t.Run("WithFieldsChecker", func(t *testing.T) {
		ds := NewDataSet(WithFieldsChecker())
		ds.SetFields("id", "name")
		err := ds.NewRecord(map[string]any{"id": 1, "age": 10})
		if err == nil {
			t.Error("Expected error when adding field not in fieldsIndex with checker enabled")
		}
	})
}

func TestDatasetCursor(t *testing.T) {
	ds := NewDataSet(WithData(
		map[string]any{"id": 1},
		map[string]any{"id": 2},
		map[string]any{"id": 3},
	))

	if ds.Position() != 0 {
		t.Errorf("Expected initial position 0, got %d", ds.Position())
	}

	ds.Next()
	if ds.Position() != 1 {
		t.Errorf("Expected position 1 after Next, got %d", ds.Position())
	}

	ds.First()
	if ds.Position() != 0 {
		t.Errorf("Expected position 0 after First, got %d", ds.Position())
	}

	if ds.Eof() {
		t.Error("Eof should be false at start")
	}

	ds.Next()
	ds.Next()
	if ds.Eof() {
		t.Error("Eof should be false at last record")
	}

	ds.Next()
	if !ds.Eof() {
		t.Error("Eof should be true after last record")
	}
}

func TestDatasetRecordManagement(t *testing.T) {
	ds := NewDataSet()
	
	t.Run("AppendRecord", func(t *testing.T) {
		rec := NewRecordSet(map[string]any{"id": 1, "name": "a"})
		err := ds.AppendRecord(rec)
		if err != nil {
			t.Errorf("AppendRecord failed: %v", err)
		}
		if ds.Count() != 1 {
			t.Errorf("Expected 1 record, got %d", ds.Count())
		}
	})

	t.Run("Delete", func(t *testing.T) {
		ds.NewRecord(map[string]any{"id": 2})
		countBefore := ds.Count()
		ds.First()
		ok := ds.Delete() // Delete current position (0)
		if !ok {
			t.Error("Delete current failed")
		}
		if ds.Count() != countBefore-1 {
			t.Errorf("Expected %d records, got %d", countBefore-1, ds.Count())
		}
	})

	t.Run("Clear", func(t *testing.T) {
		ds.NewRecord(map[string]any{"id": 3})
		ds.Clear()
		if ds.Count() != 0 {
			t.Errorf("Expected 0 records after Clear, got %d", ds.Count())
		}
		if ds.Position() != 0 {
			t.Errorf("Expected position 0 after Clear, got %d", ds.Position())
		}
	})
}

func TestDatasetRetrieval(t *testing.T) {
	ds := NewDataSet(WithData(
		map[string]any{"id": 10, "name": "ten"},
		map[string]any{"id": 20, "name": "twenty"},
	))

	t.Run("RecordByField", func(t *testing.T) {
		rec := ds.RecordByField("id", 10)
		if rec == nil {
			t.Fatal("RecordByField(id, 10) returned nil")
		}
		if rec.GetByField("name") != "ten" {
			t.Errorf("Expected 'ten', got %v", rec.GetByField("name"))
		}
	})

	t.Run("RecordByKey", func(t *testing.T) {
		ds.SetKeyField("id")
		rec := ds.RecordByKey(20)
		if rec == nil {
			t.Fatal("RecordByKey(20) returned nil")
		}
		if rec.GetByField("name") != "twenty" {
			t.Errorf("Expected 'twenty', got %v", rec.GetByField("name"))
		}
	})

	t.Run("ValueBy", func(t *testing.T) {
		values := ds.ValueBy("name")
		if len(values) != 2 {
			t.Errorf("Expected 2 values, got %d", len(values))
		}
		if values[0] != "ten" || values[1] != "twenty" {
			t.Errorf("Unexpected values: %v", values)
		}
	})
}

func TestDatasetQuery(t *testing.T) {
	ds := NewDataSet(WithData(
		map[string]any{"id": 1, "cat": "A", "val": 10},
		map[string]any{"id": 2, "cat": "B", "val": 20},
		map[string]any{"id": 3, "cat": "A", "val": 30},
	))

	t.Run("GroupBy", func(t *testing.T) {
		groups := ds.GroupBy("cat")
		if len(groups) != 2 {
			t.Errorf("Expected 2 groups, got %d", len(groups))
		}
		if len(groups["A"].Data) != 2 {
			t.Errorf("Expected 2 records in group A, got %d", len(groups["A"].Data))
		}
	})

	t.Run("Filter", func(t *testing.T) {
		filtered := ds.Filter("cat", []any{"A"})
		if filtered.Count() != 2 {
			t.Errorf("Expected 2 records (A), got %d", filtered.Count())
		}
	})
}

func TestRecordSetExtended(t *testing.T) {
	data := map[string]any{"id": 1, "name": "test", "score": 95.5}
	rec := NewRecordSet(data)

	t.Run("FieldAccess", func(t *testing.T) {
		if rec.GetByField("name") != "test" {
			t.Errorf("Expected 'test', got %v", rec.GetByField("name"))
		}
		idx := rec.GetFieldIndex("score")
		if rec.GetByIndex(idx) != 95.5 {
			t.Errorf("Expected 95.5, got %v", rec.GetByIndex(idx))
		}
	})

	t.Run("SetByField", func(t *testing.T) {
		rec.SetByField("new_field", "val")
		if rec.GetByField("new_field") != "val" {
			t.Error("SetByField failed to add new field")
		}
	})

	t.Run("Conversions", func(t *testing.T) {
		m := rec.AsMap()
		if m["id"] != 1 {
			t.Errorf("AsMap failed, id=%v", m["id"])
		}

		sm := rec.AsStrMap()
		if sm["score"] != "95.5" {
			t.Errorf("AsStrMap failed, score=%v", sm["score"])
		}

		js, err := rec.AsJson()
		if err != nil {
			t.Fatalf("AsJson failed: %v", err)
		}
		var decoded map[string]any
		json.Unmarshal([]byte(js), &decoded)
		if decoded["id"].(float64) != 1 {
			t.Errorf("Json content mismatch: %v", decoded["id"])
		}
	})

	t.Run("AsStruct", func(t *testing.T) {
		type TestStruct struct {
			ID   int     `field:"id"`
			Name string  `field:"name"`
		}
		var ts TestStruct
		err := rec.AsStruct(&ts)
		if err != nil {
			t.Fatalf("AsStruct failed: %v", err)
		}
		if ts.ID != 1 || ts.Name != "test" {
			t.Errorf("Struct values mismatch: %+v", ts)
		}
	})
}

func TestFieldSetExtended(t *testing.T) {
	now := time.Now()
	rec := NewRecordSet(map[string]any{
		"int":   123,
		"str":   "hello",
		"bool":  true,
		"float": 45.67,
		"time":  now,
		"nil":   nil,
		"ds_map": map[string]any{"id": 1, "val": "map"},
	})

	t.Run("Conversions", func(t *testing.T) {
		if rec.FieldByName("int").AsInteger() != 123 {
			t.Errorf("AsInteger failed")
		}
		if rec.FieldByName("str").AsString() != "hello" {
			t.Errorf("AsString failed")
		}
		if !rec.FieldByName("bool").AsBoolean() {
			t.Errorf("AsBoolean failed")
		}
		if rec.FieldByName("float").AsFloat() != 45.67 {
			t.Errorf("AsFloat failed")
		}
		// Time comparison can be tricky due to precision, but utils.ToTime should handle basic now
		if rec.FieldByName("time").AsDateTime().Unix() != now.Unix() {
			t.Errorf("AsDateTime failed")
		}
	})

	t.Run("IsNull", func(t *testing.T) {
		if !rec.FieldByName("nil").IsNull() {
			t.Error("IsNull should be true for nil field")
		}
		if rec.FieldByName("int").IsNull() {
			t.Error("IsNull should be false for non-nil field")
		}
	})

	t.Run("AsDataset", func(t *testing.T) {
		nestedDs := rec.FieldByName("ds_map").AsDataset()
		if nestedDs == nil {
			t.Fatal("AsDataset returned nil for map")
		}
		if nestedDs.Count() != 1 {
			t.Errorf("Expected 1 record in nested dataset, got %d", nestedDs.Count())
		}
		if nestedDs.Record().GetByField("id") != 1 {
			t.Errorf("Nested dataset content mismatch")
		}
	})
}
