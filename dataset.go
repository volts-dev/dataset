package dataset

import (
	"sync"

	"github.com/volts-dev/utils"

	"github.com/volts-dev/logger"
)

// TODO　使用全局池回收利用

const PKG_NAME = "[dataset.go]"

type (
	TDataSet struct {
		Name         string                      // table name
		Data         []*TRecordSet               // []map[string]interface{}
		fields       map[string]*TFieldSet       //保存字段
		KeyField     string                      // 主键字段
		RecordsIndex map[interface{}]*TRecordSet // 主键引索 // for RecordByKey() Keys()
		Position     int                         // 游标
		FieldCount   int                         //字段数

		// classic 字段存储的数据包含有 Struct/Array/map 等
		classic      bool // 是否存储着经典模式的数据 many2one字段会显示ID和Name
		positionLock sync.RWMutex
	}
)

func NewDataSet() *TDataSet {
	return &TDataSet{
		Position:     0,
		Data:         make([]*TRecordSet, 0),
		fields:       make(map[string]*TFieldSet),
		RecordsIndex: make(map[interface{}]*TRecordSet),
	}
}
func (self *TDataSet) Classic(value ...bool) bool {
	if len(value) > 0 {
		self.classic = value[0]
	}

	return self.classic
}

//TODO  当TDataSet无数据是返回错误
//TODO HasField()bool
func (self *TDataSet) FieldByName(field string) (fieldSet *TFieldSet) {
	var has bool
	if fieldSet, has = self.fields[field]; has {
		fieldSet.RecSet = self.Record() // point to the current record self.Data[self.Position]
		return
	} else {
		// 创建一个空的
		fieldSet = newFieldSet(field, self.Record())
	}

	return
}

//
func (self *TDataSet) IsEmpty() bool {
	return len(self.Data) == 0
}

// return the number of data
func (self *TDataSet) Count() int {
	return len(self.Data)
}

// clear all records
func (self *TDataSet) Clear() {
	self.Data = nil
	self.First()
}

// set the Pos on first
func (self *TDataSet) First() {
	self.positionLock.Lock()
	self.Position = 0
	self.positionLock.Unlock()
}

// goto next record
func (self *TDataSet) Next() {
	self.positionLock.Lock()
	self.Position++
	self.positionLock.Unlock()
}

// is the end of the data list
func (self *TDataSet) Eof() bool {
	return self.Position == len(self.Data)
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
		if rs := self.Data[self.Position]; rs != nil {
			return rs
		}
	}

	return nil
}

// #检验字段合法
//TODO 简化
func (self *TDataSet) check_fields(record *TRecordSet) error {
	// #优先记录该数据集的字段
	if len(self.fields) == 0 && self.Count() < 1 {
		for _, field := range record.Fields() {
			if field != "" { // TODO 不应该有空值 需检查
				fieldSet := newFieldSet(field, nil)
				self.fields[field] = fieldSet
			}
		}

		//# 添加字段长度
		self.FieldCount = len(self.fields)
		return nil
	}

	//#检验字段合法
	for _, field := range record.Fields() {
		if field != "" {
			if _, has := self.fields[field]; !has {
				return logger.Errf("The field name < %s > is not in this dataset! please to set field by < dataset.SetFields >", field)
			}
		}
	}

	return nil
}

// NOTE 第一条记录决定空dataset的fields
// appending a record.Its fields will be come the standard format when it is the first record of this set
func (self *TDataSet) AppendRecord(records ...*TRecordSet) error {
	for _, rec := range records {
		if rec == nil {
			continue
		}

		//if fields == nil {
		//	fields = rec.NameIndex
		//}

		if err := self.check_fields(rec); err != nil {
			logger.Err(err)

		} else { //#TODO 考虑是否为复制
			rec.dataset = self //# 将其归为
			self.Data = append(self.Data, rec)
			self.Position = len(self.Data) - 1
		}
	}

	return nil
}

//push row to dataset
func (self *TDataSet) NewRecord(record map[string]interface{}) bool {
	rec := NewRecordSet(record)
	self.AppendRecord(rec)

	return true
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

// inverse : the result will select inverse
func (self *TDataSet) Filter(field string, values []interface{}, inverse ...bool) *TDataSet {
	if field == "" || value == nil {
		return nil
	}

	inv := false
	if len(inverse) > 0 {
		inv = inverse[0]
	}

	newDataSet := NewDataSet()
	for _, rec = range self.Data {
		i := rec.FieldIndex(field)
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
		i := rec.FieldIndex(field)
		if rec.get(i, false) == val {
			return rec
		}
	}
	return
}

// 获取对应KeyFieldd值
func (self *TDataSet) RecordByKey(Key interface{}, key_field ...string) *TRecordSet {
	if len(self.RecordsIndex) == 0 {
		if self.KeyField == "" {
			if len(key_field) == 0 {
				logger.Warnf(`You should point out the key_field name!`) //#重要提示
			} else {
				if !self.SetKeyField(key_field[0]) {
					logger.Warnf(`Set key_field fail when call RecordByKey(key_field:%v)!`, key_field[0])
				}
			}
		} else {
			if !self.SetKeyField(self.KeyField) {
				logger.Warnf(`Set key_field fail when call RecordByKey(self.KeyField:%v)!`, self.KeyField)
			}
		}
	}

	//idx := self.RecordsIndex[Key]
	return self.RecordsIndex[Key]
}

// 设置固定字段
func (self *TDataSet) SetFields(fields ...string) {
	for _, name := range fields {
		fieldSet := newFieldSet(name, nil)
		self.fields[name] = fieldSet
	}
}

// set the field as key
func (self *TDataSet) SetKeyField(key_field string) bool {
	// # 非空或非Count查询时提供多行索引
	if self.Count() == 0 || (self.FieldByName(key_field) == nil && len(self.Record().Fields()) == 1 && self.Record().FieldByName("count") != nil) {
		return false
	}

	self.KeyField = key_field

	// #全新
	self.RecordsIndex = make(map[interface{}]*TRecordSet)

	// #赋值
	for _, rec := range self.Data {
		//fmt.Println("idccc", key_field, rec, len(self.RecordsIndex))
		lIdSet := rec.FieldByName(key_field)
		//fmt.Println("idccc", key_field, lIdSet, len(self.RecordsIndex))
		if lIdSet != nil {
			self.RecordsIndex[lIdSet.AsInterface()] = rec //保存ID 对应的 Record
		}
	}

	return true
}

// classic mode is
func (self *TDataSet) IsClassic() bool {
	return self.classic
}

func (self *TDataSet) Fields() map[string]*TFieldSet {
	return self.fields
}

func (self *TDataSet) HasField(name string) bool {
	_, has := self.fields[name]
	return has
}

// return all the keys value
// 返回所有记录的主键值
func (self *TDataSet) Keys(field ...string) (res []interface{}) {
	// #默认
	lKeyField := "id"

	if self.KeyField != "" {
		lKeyField = self.KeyField
	}

	// #新的Key
	if len(field) > 0 {
		lKeyField = field[0]
	}

	if self.KeyField == lKeyField {
		if self.Count() > 0 && len(self.RecordsIndex) == 0 {
			self.SetKeyField(self.KeyField)
		}
	} else {
		self.SetKeyField(lKeyField)
	}

	res = make([]interface{}, 0)
	for key, _ := range self.RecordsIndex {
		res = append(res, key)
	}

	return
}
