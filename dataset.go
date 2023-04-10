package dataset

import (
	"sync"

	treehmap "github.com/emirpasic/gods/maps/treemap"
	"github.com/volts-dev/utils"
	"github.com/volts-dev/volts/logger"
)

// TODO　使用全局池回收利用
var log = logger.New("Dataset")

type (
	TDataSet struct {
		sync.RWMutex
		config       *Config
		Name         string        // 数据模型名称 默认为空白 特殊情况下为Model的名称
		KeyField     string        // 主键字段
		fields       []string      // 字段引索列表
		Data         []*TRecordSet //
		fieldsIndex  *treehmap.Map
		RecordsIndex *treehmap.Map // 主键引索列表 // for RecordByKey() Keys()
		Position     int           // 游标
		FieldCount   int           // 字段数
		// classic 字段存储的数据包含有 Struct/Array/map 等
		classic bool // 是否存储着经典模式的数据 many2one字段会显示ID和Name
	}
)

func newRecordsIndex() *treehmap.Map {
	return treehmap.NewWith(func(a, b interface{}) int {
		if a != b {
			return 1
		}
		return 0
	})
}

func NewDataSet(opts ...Option) *TDataSet {
	dataset := &TDataSet{
		Position:    0,
		Data:        make([]*TRecordSet, 0),
		fieldsIndex: treehmap.NewWithStringComparator(),
	}
	newConfig(dataset, opts...)
	return dataset
}

func (self *TDataSet) Classic(value ...bool) bool {
	if len(value) > 0 {
		self.classic = value[0]
	}

	return self.classic
}

// TODO 薛瑶中断机制
func (self *TDataSet) Range(fn func(pos int, record *TRecordSet) error) error {
	if self == nil {
		return nil
	}

	for i, rec := range self.Data {
		if err := fn(i, rec); err != nil {
			return err
		}
	}
	return nil
}

// TODO  当TDataSet无数据是返回错误
// TODO HasField()bool
func (self *TDataSet) FieldByName(field string) (fieldSet *TFieldSet) {
	if idx, has := self.fieldsIndex.Get(field); has {
		return newFieldSet(idx.(int), field, self.Record())
	}
	return newFieldSet(-1, field, self.Record())
}

func (self *TDataSet) IsEmpty() bool {
	return len(self.Data) == 0
}

// return the number of data
func (self *TDataSet) Count() int {
	if self == nil {
		return 0
	}
	return len(self.Data)
}

// clear all records
func (self *TDataSet) Clear() {
	self.Data = nil
	if self.RecordsIndex != nil {
		self.RecordsIndex.Clear()
	}
	self.First()
}

// set the Pos on first
func (self *TDataSet) First() {
	self.Lock()
	self.Position = 0
	self.Unlock()
}

// goto next record
func (self *TDataSet) Next() {
	self.Lock()
	self.Position++
	self.Unlock()
}

// is the end of the data list
func (self *TDataSet) Eof() bool {
	return self == nil || self.Position == len(self.Data)
}

// return the current record
func (self *TDataSet) Record() *TRecordSet {
	count := len(self.Data)
	if count == 0 || count <= self.Position {
		// 规避零界点取值
		rec := NewRecordSet()
		rec.dataset = self
		return rec
	} else {
		if rec := self.Data[self.Position]; rec != nil {
			return rec
		}
	}

	return nil
}

// #检验字段合法
// TODO 简化
func (self *TDataSet) validateFields(record *TRecordSet) {
	// #优先记录该数据集的字段
	if self.Count() == 0 && self.fieldsIndex.Size() == 0 {
		self.fields = record.Fields()
		for idx, field := range self.fields {
			if field != "" { // TODO 不应该有空值 需检查
				self.fieldsIndex.Put(field, idx)
			}
		}

		//# 添加字段长度
		self.FieldCount = self.fieldsIndex.Size()
	}

	//#检验字段合法
	if self.config.checkFields {
		for _, field := range record.Fields() {
			if field != "" {
				if _, has := self.fieldsIndex.Get(field); !has {
					logger.Errf("The field name < %v > is not in this dataset! please to set field by < dataset.SetFields >", field)
				}
			}
		}
	}
}

// NOTE:第一条记录决定空dataset的fields 默认情况下会自动舍弃多余字段的数据
// appending a record.Its fields will be come the standard format when it is the first record of this set
func (self *TDataSet) AppendRecord(records ...*TRecordSet) error {
	for _, rec := range records {
		if rec == nil {
			continue
		}

		self.validateFields(rec)

		//#TODO 考虑是否为复制
		rec.dataset = self //# 将其归为
		self.Data = append(self.Data, rec)
		self.Position = len(self.Data) - 1
	}

	// 清除索引
	if self.RecordsIndex != nil && self.RecordsIndex.Size() > 0 {
		self.RecordsIndex.Clear()
	}

	return nil
}

// push row to dataset
func (self *TDataSet) NewRecord(record map[string]interface{}) error {
	return self.AppendRecord(NewRecordSet(record))
}

func (self *TDataSet) Delete(idx ...int) bool {
	cnt := len(self.Data)
	if cnt == 0 {
		return true
	}

	pos := self.Position
	if len(idx) > 0 {
		pos = idx[0]
	}

	// 超出边界
	if pos >= cnt || pos < 0 {
		return false
	}

	self.Data = append(self.Data[:pos], self.Data[pos+1:]...)

	return true
}

// TODO implement
func (self *TDataSet) DeleteRecord(Key string) bool {
	return true
}

// TODO implement
func (self *TDataSet) EditRecord(Key string, Record map[string]interface{}) bool {
	return true
}

func (self *TDataSet) GroupBy(field string) map[any]*TDataSet {
	if field == "" || !self.HasField(field) {
		return nil
	}

	// TODO 优化FieldIndex获取减少重复使用
	groups := make(map[any]*TDataSet)
	for _, rec := range self.Data {
		i := rec.GetFieldIndex(field)
		if v := rec.get(i, false); v != nil {
			grp := groups[v]
			if grp == nil {
				grp = NewDataSet()
				groups[v] = grp
			}

			grp.AppendRecord(rec)
		}
	}
	return groups
}

// inverse : the result will select inverse
func (self *TDataSet) Filter(field string, values []interface{}, inverse ...bool) *TDataSet {
	if field == "" || values == nil {
		return nil
	}

	inv := false
	if len(inverse) > 0 {
		inv = inverse[0]
	}

	newDataSet := NewDataSet()
	for _, rec := range self.Data {
		i := rec.GetFieldIndex(field)
		val := rec.get(i, false)
		if inv && utils.IdxOfItfs(val, values...) == -1 {
			newDataSet.AppendRecord(rec)
		} else if !inv {
			newDataSet.AppendRecord(rec)
		}
	}

	return newDataSet
}

// query the record by field
func (self *TDataSet) RecordByField(field string, val interface{}) (rec *TRecordSet) {
	if field == "" || val == nil {
		return nil
	}

	for _, rec = range self.Data {
		i := rec.GetFieldIndex(field)
		if rec.get(i, false) == val {
			return rec
		}
	}
	return
}

// 获取对应KeyFieldd值
func (self *TDataSet) RecordByKey(key interface{}, key_field ...string) *TRecordSet {
	if self.RecordsIndex == nil || self.RecordsIndex.Size() != len(self.Data) {
		if self.KeyField == "" {
			if len(key_field) == 0 {
				logger.Warnf(`You should point out the key_field name!`) //#重要提示
				return nil
			} else {
				if !self.SetKeyField(key_field[0]) {
					logger.Warnf(`Set key_field fail when call RecordByKey(key_field:%v)!`, key_field[0])
					return nil
				}
			}
		} else {
			if !self.SetKeyField(self.KeyField) {
				logger.Warnf(`Set key_field fail when call RecordByKey(self.KeyField:%v)!`, self.KeyField)
				return nil
			}
		}
	}

	if val, has := self.RecordsIndex.Get(key); has {
		return val.(*TRecordSet)
	}

	return nil
}

// 设置固定字段
func (self *TDataSet) SetFields(fields ...string) {
	self.fieldsIndex.Clear()
	self.fields = fields
	for idx, name := range self.fields {
		self.fieldsIndex.Put(name, idx)
	}
}

// set the field as key
func (self *TDataSet) SetKeyField(keyField string) bool {
	// # 非空或非Count查询时提供多行索引
	if self.Count() == 0 || (self.Record().GetByField(keyField) == nil && len(self.Record().Fields()) == 1 && self.Record().FieldByName("count") != nil) {
		return false
	}

	// #全新
	if self.RecordsIndex == nil {
		self.RecordsIndex = newRecordsIndex()
	} else {
		self.RecordsIndex.Clear()
	}

	self.KeyField = keyField

	// #赋值
	for _, rec := range self.Data {
		value := rec.GetByField(keyField)
		if value != nil && !utils.IsBlank(value) {
			self.RecordsIndex.Put(value, rec) //保存ID 对应的 Record
		}
	}

	return true
}

// classic mode is
func (self *TDataSet) IsClassic() bool {
	return self.classic
}

// func (self *TDataSet) Fields() map[string]*TFieldSet {
func (self *TDataSet) Fields() []string {
	return self.fields
}

func (self *TDataSet) HasField(name string) bool {
	_, has := self.fieldsIndex.Get(name)
	return has
}

// return all the keys value
// 返回所有记录的非空非Nil主键值
func (self *TDataSet) Keys(fieldName ...string) (res []interface{}) {
	if self.Count() == 0 {
		return nil
	}

	var keyField string
	// #新的Key
	if len(fieldName) > 0 {
		keyField = fieldName[0]
		var ids []any
		for _, rec := range self.Data {
			value := rec.GetByField(keyField)
			if value != nil && !utils.IsBlank(value) {
				ids = append(ids, value)
			}
		}
		return ids
	} else {
		keyField = "id" // #默认
		if self.KeyField != "" {
			keyField = self.KeyField
		}
	}

	if self.KeyField == keyField {
		if self.Count() > 0 && (self.RecordsIndex == nil || self.RecordsIndex.Size() == 0) {
			self.SetKeyField(self.KeyField)
		}
	} else {
		self.SetKeyField(keyField)
	}

	return self.RecordsIndex.Keys()
}
