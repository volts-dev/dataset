package dataset

import (
	"encoding/json"
	"log"
	"reflect"
	"time"

	"github.com/volts-dev/utils"
)

type (
	TRecordSet struct {
		dataset       IDataSet
		fields        []string
		values        []interface{}  // []string
		ClassicValues []interface{}  // 存储经典字段值
		nameIndex     map[string]int // TODO treemap
		fieldCount    int
		isEmpty       bool
	}
)

func NewRecordSet(record ...map[string]interface{}) *TRecordSet {
	recset := &TRecordSet{}
	recset.Reset()

	if len(record) == 0 {
		return recset
	}

	var idx int
	for field, val := range record[0] {
		idx = len(recset.fields)
		recset.nameIndex[field] = idx // 先于 lRec.fields 添加不需 -1
		recset.fields = append(recset.fields, field)
		recset.values = append(recset.values, val)

	}
	//#优先计算长度供Get Set设置
	recset.fieldCount = idx + 1
	recset.isEmpty = idx == 0
	return recset
}

// reset all data to blank
func (self *TRecordSet) Reset() {
	self.dataset = nil
	self.fields = make([]string, 0)
	self.values = make([]interface{}, 0)
	self.ClassicValues = make([]interface{}, 0)
	self.nameIndex = make(map[string]int)
	self.fieldCount = 0
	self.isEmpty = true
}

func (self *TRecordSet) FieldIndex(name string) int {
	return self.nameIndex[name]
}

func (self *TRecordSet) resetByFields() {
	self.fieldCount = len(self.fields)
	self.values = make([]interface{}, self.fieldCount)

	// rebuild indexs
	for idx, name := range self.fields {
		self.nameIndex[name] = idx
	}
}

func (self *TRecordSet) Fields(fields ...string) []string {
	if fields != nil {
		//reset all
		self.fields = fields
		self.resetByFields()
	}

	return self.fields
}

// TODO 函数改为非Exported
func (self *TRecordSet) get(index int, classic bool) interface{} {
	if index >= self.fieldCount {
		return nil
	}

	return self.values[index]
}

// TODO 函数改为非Exported
func (self *TRecordSet) set(index int, value interface{}, classic bool) bool {
	if index >= self.fieldCount {
		return false
	}

	if classic {
		self.ClassicValues[index] = value
	} else {
		self.values[index] = value
	}

	return true
}
func (self *TRecordSet) Length() int {
	return self.fieldCount
}

func (self *TRecordSet) SetDataset(ds IDataSet) {
	self.dataset = ds
}

func (self *TRecordSet) GetByName(name string, classic bool) interface{} {
	if index, ok := self.nameIndex[name]; ok {
		return self.get(index, classic)
	}

	return nil
}
func (self *TRecordSet) IsEmpty() bool {
	return self.isEmpty
}

// !NOTE! 该函数仅供修改不做添加字段
func (self *TRecordSet) SetByName(name string, value interface{}, classic bool) bool {
	if index, ok := self.nameIndex[name]; ok {
		return self.set(index, value, classic)
	}

	return false
}

//字段被纳入Dataset.Fields
func (self *TRecordSet) setByName(fs *TFieldSet, name string, value interface{}, classic bool) bool {
	fs.IsValid = true

	if index, ok := self.nameIndex[name]; ok {
		return self.set(index, value, classic)
	} else {
		self.nameIndex[name] = len(self.values)
		self.fields = append(self.fields, name)
		if classic {
			self.ClassicValues = append(self.ClassicValues, value)
		} else {
			self.values = append(self.values, value)
		}

		self.fieldCount = len(self.values)
	}

	return true
}

func (self *TRecordSet) FieldByIndex(index int) *TFieldSet {
	// 检查零界
	if index >= self.fieldCount {
		return nil
	}

	field := self.fields[index]
	if self.dataset != nil {
		// 检查零界
		if len(self.dataset.Fields()) != self.fieldCount {
			return nil
		}

		res := self.dataset.Fields()[field]
		res.RecSet = self
		return res
	} else {
		// 创建一个空的
		res := newFieldSet(field, self)
		res.IsValid = field != ""
		/*res = &TFieldSet{
			//dataset: self.dataset,
			RecSet: self,
			Name:   field,
			IsNil:  true,
		}*/
		return res
	}

	return nil
}

// 获取某个
func (self *TRecordSet) FieldByName(name string) *TFieldSet {
	// 优先验证Dataset
	if self.dataset != nil {
		if field, has := self.dataset.Fields()[name]; has {
			if field != nil {
				field.RecSet = self
				return field //self.values[index]
			}
		}
	}

	// 创建一个空的
	field := newFieldSet(name, self)
	field.IsValid = utils.InStrings(name, self.fields...) != -1
	/*field = &TFieldSet{
		//dataset: self.dataset,
		RecSet: self,
		Name:   name,
		//IsNil:  true,
	}*/

	return field
}

// convert to a string map
func (self *TRecordSet) AsStrMap() map[string]string {
	m := make(map[string]string)
	for idx, field := range self.fields {
		m[field] = utils.Itf2Str(self.values[idx])
	}

	return m
}

// convert to an interface{} map
func (self *TRecordSet) AsItfMap() map[string]interface{} {
	m := make(map[string]interface{})

	for idx, field := range self.fields {
		m[field] = self.values[idx]
	}

	return m
}

// convert to a json string
func (self *TRecordSet) AsJson() (string, error) {
	js, err := json.Marshal(self.AsItfMap())
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
	// 使用经典数据模式
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
			log.Printf(PKG_NAME+"Target's filed %v@%s is not valid or cannot set IsValid:%v CanSet:%v", lStruct.Type().Name(), name, lFieldValue.IsValid(), lFieldValue.CanSet())
			continue
		}

		//lFieldType := lFieldValue.Type()
		var lItfVal interface{}
		var lVal reflect.Value
		if lClassic {
			//self.dataset.
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
				lItfVal = utils.Itf2Str(lItfVal)
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
				log.Printf(PKG_NAME+"Unsupported struct type %v", lFieldValue.Type().Kind())
				continue
			}
		}

		lVal = reflect.ValueOf(lItfVal)
		lFieldValue.Set(lVal)
	}

	return nil
}

func (self *TRecordSet) MergeToStrMap(target map[string]string) (res map[string]string) {
	for idx, field := range self.fields {
		target[field] = utils.Itf2Str(self.values[idx])
	}

	return target
}
