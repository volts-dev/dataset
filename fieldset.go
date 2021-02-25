package dataset

import (
	"time"

	"github.com/volts-dev/logger"
	"github.com/volts-dev/utils"
)

type (
	TFieldSet struct {
		RecSet  *TRecordSet
		Name    string
		IsValid bool // the field is using on dataset or temp field
	}
)

func newFieldSet(name string, recset *TRecordSet) *TFieldSet {
	return &TFieldSet{
		RecSet:  recset,
		Name:    name,
		IsValid: false,
	}
}

func (self *TFieldSet) AsInterface(value ...interface{}) (result interface{}) {
	if self == nil {
		logger.Warnf("Can not covert value into interface{} since the field is invalidation!")
		return
	}

	if len(value) != 0 {
		self.RecSet.setByName(self, self.Name, value[0], false)
		return value[0]
	}

	return self.RecSet.GetByName(self.Name, false)
}

func (self *TFieldSet) AsClassic(value ...interface{}) interface{} {
	if self == nil {
		logger.Warnf("Can not covert value into interface{} since the field is invalidation!")
		return nil
	}

	if len(value) != 0 {
		self.RecSet.setByName(self, self.Name, value[0], true)
		return value[0]
	}

	return self.RecSet.GetByName(self.Name, true)
}

//
func (self *TFieldSet) AsString(value ...string) string {
	if self == nil {
		logger.Warnf("Can not covert value into string since the field is invalidation!")
		return ""
	}

	if len(value) != 0 {
		self.RecSet.setByName(self, self.Name, value[0], false)
		return value[0]
	}

	return utils.Itf2Str(self.RecSet.GetByName(self.Name, false))
}

// TODO
func (self *TFieldSet) AsDataset(value ...*TDataSet) *TDataSet {
	if self == nil {
		logger.Warnf("Can not covert value into string since the field is invalidation!")
		return nil
	}

	if len(value) != 0 {
		self.RecSet.setByName(self, self.Name, value[0], false)
		return value[0]
	}

	return nil
}

func (self *TFieldSet) AsInteger(value ...int64) (result int64) {
	if self == nil {
		logger.Warnf("Can not covert value into int64 since the field is invalidation!")
		return 0
	}

	if len(value) != 0 {
		self.RecSet.setByName(self, self.Name, value[0], false)
		return value[0]
	}

	return utils.Itf2Int64(self.RecSet.GetByName(self.Name, false))
}

// set/get value of field as bool type
func (self *TFieldSet) AsBoolean(value ...bool) bool {
	if self == nil {
		logger.Warnf("Can not covert value into bool since the field is invalidation!")
		return false
	}

	if len(value) != 0 {
		self.RecSet.setByName(self, self.Name, value[0], false)
		return value[0]
	}

	return utils.Itf2Bool(self.RecSet.GetByName(self.Name, false))
}

func (self *TFieldSet) AsDateTime(value ...time.Time) time.Time {
	if self == nil {
		logger.Warnf("Can not covert value into time.Time since the field is invalidation!")
		return time.Time{}
	}

	if len(value) != 0 {
		self.RecSet.setByName(self, self.Name, value[0].Format(time.RFC3339), false)
		return value[0]

	}

	return utils.Itf2Time(self.RecSet.GetByName(self.Name, false))
}

func (self *TFieldSet) AsFloat(value ...float64) (result float64) {
	if self == nil {
		logger.Warnf("Can not covert value into float64 since the field is invalidation!")
		return 0.0
	}

	if len(value) != 0 {
		self.RecSet.setByName(self, self.Name, value[0], false)
		return value[0]
	}

	return utils.Itf2Float(self.RecSet.GetByName(self.Name, false))
}
