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

	return self.values[index]
}

func (self *TRecordSet) set(index int, value interface{}, classic bool) bool {
	if index < 0 || index >= self.fieldsCount {
		return false
	}

	if classic {
		self.ClassicValues[index] = value
	} else {
		self.values[index] = value
	}

	return true
}

// 重置记录字段索引
func (self *TRecordSet) resetByFields(fields ...string) {
	self.values = make([]interface{}, len(fields))
	self.ClassicValues = make([]interface{}, 0)

	// rebuild indexs
	self.fieldsIndex = make(map[string]int) // treehmap.NewWithStringComparator()
	for idx, name := range fields {
		self.fieldsIndex[name] = idx
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
	if len(classic) > 1 {
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
		if len(classic) > 1 {
			isclassic = classic[0]
		}

		return self.get(index, isclassic)
	}

	return nil
}

func (self *TRecordSet) IsEmpty() bool {
	return self == nil || self.getFieldsIndex() == nil || self.fieldsCount == 0 //|| self.isEmpty
}

// !NOTE! 该函数仅供修改不做添加字段
// 字段被纳入Dataset.Fields
func (self *TRecordSet) SetByField(field string, value interface{}, classic ...bool) bool {
	var isclassic bool
	if len(classic) > 1 {
		isclassic = classic[0]
	}

	// 如果是一个单独非dataset下的记录
	if self.dataset != nil && self.dataset.config.checkFields && self.index != -1 && !self.dataset.HasField(field) {
		//log.Errf("The field name < %s > is not in this dataset! please to set field by < dataset.SetFields >", field)
		return false
	}

	fieldsIdx := self.getFieldsIndex()
	if fieldsIdx == nil {
		self.fieldsIndex = make(map[string]int)
		fieldsIdx = self.fieldsIndex
	}

	if index, ok := fieldsIdx[field]; ok {
		self.set(index, value, isclassic)
	} else {
		// New field requires standalone map
		if self.fieldsIndex == nil && self.dataset != nil {
			// Migrate from dataset sharing to standalone
			self.fieldsIndex = make(map[string]int)
			for k, v := range self.dataset.fieldsIndex {
				self.fieldsIndex[k] = v
			}
			fieldsIdx = self.fieldsIndex
		}

		fieldsIdx[field] = self.fieldsCount
		if isclassic {
			self.ClassicValues = append(self.ClassicValues, value)
		} else {
			self.values = append(self.values, value)
		}
		self.fieldsCount++
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
	for field, value := range fieldsIdx {
		m[field] = utils.ToString(self.values[value])
	}

	return m
}

// convert to an interface{} map
func (self *TRecordSet) AsMap() map[string]interface{} {
	m := make(map[string]interface{})
	fieldsIdx := self.getFieldsIndex()
	for field, value := range fieldsIdx {
		m[field] = self.values[value]
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
// the terget must be a pointer value
func (self *TRecordSet) AsStruct(target interface{}, classic ...bool) error {
	if target == (interface{})(nil) {
		return nil
	}

	decode(self.AsMap(), target)

	/*// 使用经典数据模式
	lClassic := false
	if len(classic) > 0 {
		lClassic = classic[0]
	}


		lStruct := reflect.Indirect(reflect.ValueOf(target))
		if lStruct.Kind() == reflect.Ptr {
			lStruct = lStruct.Elem()
		}

		for idx, name := range self.fields {
			lFieldValue := lStruct.FieldByName(utils.TitleCasedName(name))
			if !lFieldValue.IsValid() || !lFieldValue.CanSet() {
				log.Errf("the field of %v@%s is not valid or cannot set IsValid:%v CanSet:%v", lStruct.Type().Name(), name, lFieldValue.IsValid(), lFieldValue.CanSet())
				continue
			}

			//lFieldType := lFieldValue.Type()
			var lItfVal interface{}
			var lVal reflect.Value
			if lClassic {
				//lVal = reflect.ValueOf(self.ClassicValues[idx])
				lItfVal = self.ClassicValues[idx]
			} else {
				//lVal = reflect.ValueOf(self.values[idx])
				lItfVal = self.values[idx]
			}

			// 不设置Nil值
			if lItfVal == nil {
				continue
			}

			// TODO 优化转化
			//logger.Dbg("AsStruct", name, lFieldValue.Type(), lItfVal, reflect.TypeOf(lItfVal), lVal, self.values[idx])
			if lFieldValue.Type().Kind() != reflect.TypeOf(lItfVal).Kind() {
				switch lFieldValue.Type().Kind() {
				case reflect.Bool:
					lItfVal = utils.Itf2Bool(lItfVal)
				case reflect.String:
					lItfVal = utils.ToString(lItfVal)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
					lItfVal = utils.Itf2Int(lItfVal)
				case reflect.Int64:
					lItfVal = utils.Itf2Int64(lItfVal)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
					lItfVal = utils.Itf2Int(lItfVal)
				case reflect.Uint64:
					lItfVal = utils.Itf2Int64(lItfVal)
				case reflect.Float32:
					lItfVal = utils.Itf2Float32(lItfVal)
				case reflect.Float64:
					lItfVal = utils.Itf2Float(lItfVal)
				//case reflect.Array, reflect.Slice:
				case reflect.Struct:
					var c_TIME_DEFAULT time.Time
					TimeType := reflect.TypeOf(c_TIME_DEFAULT)
					if lFieldValue.Type().ConvertibleTo(TimeType) {
						lItfVal = utils.Itf2Time(lItfVal)
					}
				default:
					log.Errf("Unsupported struct type %v", lFieldValue.Type().Kind())
					continue
				}
			}

			lVal = reflect.ValueOf(lItfVal)
			lFieldValue.Set(lVal)
		}
	*/

	return nil
}

func (self *TRecordSet) MergeToMap(target map[string]string) (res map[string]string) {
	/*	for idx, field := range self.fields {
			target[field] = utils.ToString(self.values[idx])
		}
	*/
	return target
}

func decode(input any, output any) error {
	cfg := &structmap.DecoderConfig{
		Metadata: nil,
		Result:   output,
		TagName:  "field",
	}

	decoder, err := structmap.NewDecoder(cfg)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}
