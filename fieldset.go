package dataset

import (
	"time"

	"github.com/volts-dev/utils"
)

type (
	TFieldSet struct {
		RecSet  *TRecordSet
		Name    string
		Index   int  // the index of the recordset values, -1 piont to the nil recordset
		IsValid bool // the field is using on dataset or temp field
	}
)

func newFieldSet(idx int, name string, recset *TRecordSet) *TFieldSet {
	return &TFieldSet{
		RecSet:  recset,
		Name:    name,
		Index:   idx,
		IsValid: true,
	}
}

func (self *TFieldSet) AsInterface() (result interface{}) {
	if self == nil {
		log.Warnf("Can not covert value into interface{} since the field is invalidation!")
		return
	}

	return self.RecSet.GetByField(self.Name, false)
}

func (self *TFieldSet) AsBytes() []byte {
	if self == nil {
		log.Warnf("Can not covert value into string since the field is invalidation!")
		return nil
	}

	return []byte(utils.ToString(self.RecSet.GetByField(self.Name, false)))
}

func (self *TFieldSet) AsString() string {
	if self == nil {
		log.Warnf("Can not covert value into string since the field is invalidation!")
		return ""
	}

	return utils.ToString(self.RecSet.GetByField(self.Name, false))
}

// TODO
func (self *TFieldSet) AsDataset() *TDataSet {
	if self == nil {
		log.Warnf("Can not covert value into string since the field is invalidation!")
		return nil
	}

	value := self.RecSet.GetByField(self.Name, false)
	switch v := value.(type) {
	case map[string]any:
		return NewDataSet(
			WithData(v),
		)
	case []map[string]any:
		return NewDataSet(
			WithData(v...),
		)
	case []any:
		if _, ok := v[0].(map[string]any); ok {
			ds := NewDataSet()
			for _, m := range v {
				ds.NewRecord(m.(map[string]any))
			}

			return ds
		}
	}

	return nil
}

// 是否为空值
func (self *TFieldSet) IsNull() bool {
	if v := self.RecSet.GetByField(self.Name, false); v != nil {
		return utils.IsBlank(v) // TODO 是否应该判定空值
	}

	return true
}

func (self *TFieldSet) AsInteger() (result int64) {
	if self == nil {
		log.Warnf("Can not covert value into int64 since the field is invalidation!")
		return 0
	}

	return utils.ToInt64(self.RecSet.GetByField(self.Name, false))
}

// set/get value of field as bool type
func (self *TFieldSet) AsBoolean() bool {
	if self == nil {
		log.Warnf("Can not covert value into bool since the field is invalidation!")
		return false
	}

	return utils.ToBool(self.RecSet.GetByField(self.Name, false))
}

func (self *TFieldSet) AsDateTime() time.Time {
	if self == nil {
		log.Warnf("Can not covert value into time.Time since the field is invalidation!")
		return time.Time{}
	}

	return utils.ToTime(self.RecSet.GetByField(self.Name, false))
}

func (self *TFieldSet) AsFloat() (result float64) {
	if self == nil {
		log.Warnf("Can not covert value into float64 since the field is invalidation!")
		return 0.0
	}

	return utils.ToFloat64(self.RecSet.GetByField(self.Name, false))
}
