package dataset

import (
	"encoding/json"
	"sync"

	structmap "github.com/mitchellh/mapstructure"
	"github.com/volts-dev/utils"
)

type (
	TRecordSet struct {
		dataset       *TDataSet
		values        []interface{} // []string
		ClassicValues []interface{} // 存储经典字段值
		fieldsIndex   map[string]int
		fieldsCount   int
		index         int // the index of dataset.data
	}
)

var recordSetPool = sync.Pool{
	New: func() interface{} {
		return &TRecordSet{
			index: -1,
		}
	},
}

func NewRecordSet(record ...map[string]interface{}) *TRecordSet {
	recset := recordSetPool.Get().(*TRecordSet)
	recset.Reset()

	if len(record) == 0 {
		return recset
	}

	recset.fieldsIndex = make(map[string]int)
	idx := 0
	for field, val := range record[0] {
		recset.fieldsIndex[field] = idx
		recset.values = append(recset.values, val)
		idx++
	}

	recset.fieldsCount = idx
	return recset
}

func (self *TRecordSet) get(index int, classic bool) interface{} {
	if index < 0 || index >= self.fieldsCount {
		return nil
	}

	if classic {
		if index >= len(self.ClassicValues) {
			return nil
		}
		return self.ClassicValues[index]
	}

	if index >= len(self.values) {
		return nil
	}

	return self.values[index]
}

func (self *TRecordSet) set(index int, value interface{}, classic bool) bool {
	if index < 0 || index >= 255 {
		return false
	}

	if classic {
		if index >= len(self.ClassicValues) {
			// Grow ClassicValues
			newSize := index + 1
			newVals := make([]interface{}, newSize)
			copy(newVals, self.ClassicValues)
			self.ClassicValues = newVals
		}
		self.ClassicValues[index] = value
	} else {
		if index >= len(self.values) {
			// Grow values
			newSize := index + 1
			newVals := make([]interface{}, newSize)
			copy(newVals, self.values)
			self.values = newVals
		}
		self.values[index] = value
	}

	if index >= self.fieldsCount {
		self.fieldsCount = index + 1
	}

	return true
}

// 重置记录字段索引
func (self *TRecordSet) resetByFields(fields ...string) {
	self.values = make([]interface{}, len(fields))
	self.ClassicValues = make([]interface{}, 0)

	// 如果有具体字段则说明非 dataset 寄托，需为其独立建立索引结构
	if len(fields) > 0 {
		self.fieldsIndex = make(map[string]int)
		for idx, name := range fields {
			self.fieldsIndex[name] = idx
		}
	} else if self.dataset != nil {
		// 如果有寄托的 dataset，将其指向 dataset，节约独立维护 map 的开销
		self.fieldsIndex = nil
	} else {
		self.fieldsIndex = make(map[string]int)
	}

	self.fieldsCount = len(fields)
}

func (self *TRecordSet) getFieldsIndex() map[string]int {
	if self.fieldsIndex != nil {
		return self.fieldsIndex
	}
	if self.dataset != nil {
		return self.dataset.fieldsIndex
	}
	return nil
}

// reset all data to blank
func (self *TRecordSet) Reset() {
	self.dataset = nil
	self.index = -1
	self.fieldsIndex = nil // reset fieldsIndex explicitly
	self.resetByFields()
}

func (self *TRecordSet) Free() {
	self.Reset()
	// Optionally clear references to allow GC, though Reset already clears values if resetByFields drops things.
	self.fieldsIndex = nil
	self.values = nil
	self.ClassicValues = nil
	self.fieldsCount = 0
	recordSetPool.Put(self)
}

func (self *TRecordSet) Fields(fields ...string) []string {
	if fields != nil {
		self.resetByFields(fields...)
	}

	var res []string
	fieldsIdx := self.getFieldsIndex()
	for field := range fieldsIdx {
		res = append(res, field)
	}

	return res
}

// the record length
func (self *TRecordSet) Length() int {
	return self.fieldsCount
}

func (self *TRecordSet) SetDataset(dataset *TDataSet) {
	self.dataset = dataset
	if self.fieldsCount == 0 {
	}
}

func (self *TRecordSet) GetFieldIndex(name string) int {
	fieldsIdx := self.getFieldsIndex()
	if fieldsIdx == nil {
		return -1
	}
	idx, ok := fieldsIdx[name]
	if !ok {
		return -1 // 或者定义一个常量表示未找到，如 math.MinInt32
	}

	return idx
}

func (self *TRecordSet) GetByIndex(index int, classic ...bool) interface{} {
	var isclassic bool
	if len(classic) > 0 {
		isclassic = classic[0]
	}
	return self.get(index, isclassic)
}

func (self *TRecordSet) GetByField(name string, classic ...bool) interface{} {
	fieldsIdx := self.getFieldsIndex()
	if fieldsIdx == nil {
		return nil
	}

	if index, ok := fieldsIdx[name]; ok {
		var isclassic bool
		if len(classic) > 0 {
			isclassic = classic[0]
		}

		return self.get(index, isclassic)
	}

	return nil
}

func (self *TRecordSet) IsEmpty() bool {
	return self == nil || self.getFieldsIndex() == nil || self.fieldsCount == 0 //|| self.isEmpty
}

// !NOTE! 该函数支持动态添加字段
// 字段被纳入Dataset.Fields
func (self *TRecordSet) SetByField(field string, value interface{}, classic ...bool) bool {
	var isclassic bool
	if len(classic) > 0 {
		isclassic = classic[0]
	}

	// 权限检查
	if self.dataset != nil && self.dataset.config.checkFields && !self.dataset.HasField(field) {
		return false
	}

	fieldsIdx := self.getFieldsIndex()
	if fieldsIdx == nil {
		self.fieldsIndex = make(map[string]int)
		fieldsIdx = self.fieldsIndex
	}

	index, ok := fieldsIdx[field]
	if !ok {
		// 新字段处理
		if self.dataset != nil {
			// 如果隶属于 Dataset，则尝试在 Dataset 中新增字段
			index = self.dataset.AddField(field)
			if index == -1 {
				return false
			}
		} else {
			// 独立记录，直接在记录级别新增
			if len(fieldsIdx) >= 255 {
				return false
			}
			index = len(fieldsIdx)
			fieldsIdx[field] = index
		}
	}

	// 调用 set 执行设值（内部处理增长）
	if !self.set(index, value, isclassic) {
		return false
	}

	// 插入新记录到dataset
	if self.dataset != nil && self.index == -1 {
		if _, has := self.dataset.fieldsIndex[field]; !has {
			self.dataset.fieldsIndex[field] = len(self.dataset.fieldsIndex)
			self.dataset.fields = nil
			for field := range self.dataset.fieldsIndex {
				self.dataset.fields = append(self.dataset.fields, utils.ToString(field))
			}
		}

		self.dataset.AppendRecord(self)      // 插入数据后Position会变更到当前记录
		self.index = self.dataset.Position() // 记录当前索引值
	}

	return true
}

func (self *TRecordSet) FieldByIndex(idx int) *TFieldSet {
	var fieldName string
	var value int
	fieldsIdx := self.getFieldsIndex()
	for fieldName, value = range fieldsIdx {
		if value == idx {
			break
		}
	}

	return newFieldSet(idx, fieldName, self)
}

// 获取某个
func (self *TRecordSet) FieldByName(name string) *TFieldSet {
	fieldsIdx := self.getFieldsIndex()
	if fieldsIdx != nil {
		if idx, has := fieldsIdx[name]; has {
			return newFieldSet(idx, name, self)
		}
	}

	// 创建一个空的
	field := newFieldSet(-1, name, self)
	if fieldsIdx != nil {
		_, field.IsValid = fieldsIdx[name]
	}
	return field
}

// convert to a string map
func (self *TRecordSet) AsStrMap() map[string]string {
	m := make(map[string]string)
	fieldsIdx := self.getFieldsIndex()
	for field := range fieldsIdx {
		m[field] = utils.ToString(self.GetByField(field))
	}

	return m
}

// convert to an interface{} map
func (self *TRecordSet) AsMap() map[string]interface{} {
	m := make(map[string]interface{})
	fieldsIdx := self.getFieldsIndex()
	for field := range fieldsIdx {
		m[field] = self.GetByField(field)
	}

	return m
}

// convert to a json string
func (self *TRecordSet) AsJson() (string, error) {
	js, err := json.Marshal(self.AsMap())
	if err != nil {
		return "", err
	}

	return string(js), nil
}

// TODO AsXml
func (self *TRecordSet) AsXml() (res string) {
	return
}

// TODO AsCsv
func (self *TRecordSet) AsCsv() (res string) {
	return
}

// mapping to a struct
// the target must be a pointer value
func (self *TRecordSet) AsStruct(target interface{}, classic ...bool) error {
	if target == (interface{})(nil) {
		return nil
	}

	return decode(self.AsMap(), target)
}

func (self *TRecordSet) MergeToMap(target map[string]string) (res map[string]string) {
	fieldsIdx := self.getFieldsIndex()
	for field := range fieldsIdx {
		target[field] = utils.ToString(self.GetByField(field))
	}

	return target
}

func decode(input any, output any) error {
	cfg := &structmap.DecoderConfig{
		Metadata:         nil,
		Result:           output,
		TagName:          "field",
		WeaklyTypedInput: true,
	}

	decoder, err := structmap.NewDecoder(cfg)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}
