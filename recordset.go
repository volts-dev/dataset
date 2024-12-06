package dataset

import (
	"encoding/json"

	treehmap "github.com/emirpasic/gods/maps/treemap"
	structmap "github.com/mitchellh/mapstructure"
	"github.com/volts-dev/utils"
)

type (
	TRecordSet struct {
		dataset       *TDataSet
		values        []interface{} // []string
		ClassicValues []interface{} // 存储经典字段值
		fieldsIndex   *treehmap.Map
		//isEmpty       bool
		index int // the index of dataset.data
	}
)

func NewRecordSet(record ...map[string]interface{}) *TRecordSet {
	recset := &TRecordSet{
		index:       -1,
		fieldsIndex: treehmap.NewWithStringComparator(),
	}
	recset.Reset()

	if len(record) == 0 {
		return recset
	}

	for field, val := range record[0] {
		//recset.fieldsIndex[field] = idx // 先于 lRec.fields 添加不需 -1
		recset.fieldsIndex.Put(field, recset.fieldsIndex.Size())
		//recset.fields = append(recset.fields, field)
		recset.values = append(recset.values, val)

	}
	//#优先计算长度供Get Set设置
	//recset.isEmpty = recset.fieldsIndex.Size() == 0
	return recset
}

func (self *TRecordSet) get(index int, classic bool) interface{} {
	if index >= self.fieldsIndex.Size() {
		return nil
	}

	return self.values[index]
}

func (self *TRecordSet) set(index int, value interface{}, classic bool) bool {
	if index >= self.fieldsIndex.Size() {
		return false
	}

	if classic {
		self.ClassicValues[index] = value
	} else {
		self.values[index] = value
	}

	//self.isEmpty = false
	return true
}

// 重置记录字段索引
func (self *TRecordSet) resetByFields(fields ...string) {
	self.values = make([]interface{}, len(fields))

	// rebuild indexs
	self.fieldsIndex.Clear()
	for idx, name := range fields {
		self.fieldsIndex.Put(name, idx)
	}
}

// reset all data to blank
func (self *TRecordSet) Reset() {
	if self.fieldsIndex == nil {
		self.fieldsIndex = treehmap.NewWithStringComparator()
	} else {
		self.fieldsIndex.Clear() //
	}

	self.dataset = nil
	self.values = make([]interface{}, 0)
	self.ClassicValues = make([]interface{}, 0)
	//self.isEmpty = true
}

func (self *TRecordSet) Fields(fields ...string) []string {
	if fields != nil {
		//reset all
		self.resetByFields(fields...)
	}

	var res []string
	for _, field := range self.fieldsIndex.Keys() {
		res = append(res, utils.ToString(field))

	}

	return res
}

// the record length
func (self *TRecordSet) Length() int {
	return self.fieldsIndex.Size()
}

func (self *TRecordSet) SetDataset(dataset *TDataSet) {
	self.dataset = dataset
	if self.fieldsIndex.Size() == 0 {
	}
}

func (self *TRecordSet) GetFieldIndex(name string) int {
	val, _ := self.fieldsIndex.Get(name)
	return val.(int)
}

func (self *TRecordSet) GetByIndex(index int, classic ...bool) interface{} {
	var isclassic bool
	if len(classic) > 1 {
		isclassic = classic[0]
	}
	return self.get(index, isclassic)
}

func (self *TRecordSet) GetByField(name string, classic ...bool) interface{} {
	fieldsIndex := self.fieldsIndex
	// TODO Fix无法同步dataset 和 recordset index
	//if self.fieldsIndex == nil && self.dataset != nil {
	//	fieldsIndex = self.dataset.fieldsIndex
	//}

	if index, ok := fieldsIndex.Get(name); ok {
		var isclassic bool
		if len(classic) > 1 {
			isclassic = classic[0]
		}
		return self.get(index.(int), isclassic)
	}

	return nil
}

func (self *TRecordSet) IsEmpty() bool {
	return self == nil || self.fieldsIndex == nil || self.fieldsIndex.Size() == 0 //|| self.isEmpty
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
		log.Errf("The field name < %s > is not in this dataset! please to set field by < dataset.SetFields >", field)
		return false
	}

	if index, ok := self.fieldsIndex.Get(field); ok {
		self.set(index.(int), value, isclassic)
	} else {
		self.fieldsIndex.Put(field, self.fieldsIndex.Size())
		//self.fields = append(self.fields, name)
		if isclassic {
			self.ClassicValues = append(self.ClassicValues, value)
		} else {
			self.values = append(self.values, value)
		}
	}

	// 插入新记录到dataset
	if self.dataset != nil && self.index == -1 {
		if _, has := self.dataset.fieldsIndex.Get(field); !has {
			self.dataset.fieldsIndex.Put(field, self.dataset.fieldsIndex.Size())
			self.dataset.fields = nil
			for _, field := range self.dataset.fieldsIndex.Keys() {
				self.dataset.fields = append(self.dataset.fields, utils.ToString(field))
			}
		}
		self.dataset.AppendRecord(self)    // 插入数据后Position会变更到当前记录
		self.index = self.dataset.Position // 记录当前索引值
	}

	return true
}

func (self *TRecordSet) FieldByIndex(idx int) *TFieldSet {
	fieldName, _ := self.fieldsIndex.Find(func(key, value interface{}) bool {
		if value.(int) == idx {
			return true
		}
		return false
	})

	if fieldName == nil {
		// 创建一个空的
		return newFieldSet(idx, "", self)
	}

	return newFieldSet(idx, fieldName.(string), self)
}

// 获取某个
func (self *TRecordSet) FieldByName(name string) *TFieldSet {
	// 优先验证Dataset
	if self.dataset != nil {
		if i, has := self.dataset.fieldsIndex.Get(name); has {
			if i != nil {
				idx := i.(int)
				return newFieldSet(idx, name, self)
			}
		}
	}

	// 创建一个空的
	field := newFieldSet(-1, name, self)
	_, field.IsValid = self.fieldsIndex.Get(name)

	return field
}

// convert to a string map
func (self *TRecordSet) AsStrMap() map[string]string {
	m := make(map[string]string)

	self.fieldsIndex.Each(func(key, value interface{}) {
		if field, ok := key.(string); ok {
			m[field] = utils.ToString(self.values[value.(int)])
		}
	})

	return m
}

// convert to an interface{} map
func (self *TRecordSet) AsMap() map[string]interface{} {
	m := make(map[string]interface{})

	self.fieldsIndex.Each(func(key, value interface{}) {
		if field, ok := key.(string); ok {
			m[field] = self.values[value.(int)]
		}
	})

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
