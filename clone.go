package dataset

// 本文件提供 TDataSet / TRecordSet 的副本构造。
//
// 存在的理由：`*TDataSet` 是个**可变**对象——游标(position)、classic 标志、字段表
// (fields/fieldsIndex)、以及每条记录的 values 都可以被读取方就地改写。任何把同一个
// 实例交给多个消费者的场景（典型是把查询结果放进缓存再发给后续请求）都需要一份互不
// 影响的副本，而 config/classic/fieldFormater/fieldsIndex 全是未导出字段，包外用公开
// API 拼不出等价对象。

// Clone 返回本数据集的副本：字段表、格式化器、游标、classic 标志与全部记录都是新的，
// 对副本的任何改动都不会影响原数据集，反之亦然。
//
// **值本身按引用复制**（interface 头拷贝）。标量值(int64/string/time…)不可变，怎么拷
// 都一样；而经典/嵌套读取放进来的复合值(many2one→map、o2m/m2m→[]map)副本与原件仍指向
// 同一份底层数据。这对「替换整个字段值」的用法(SetByField，也是 OnRead 的做法)是安全的，
// 对「就地改写内嵌 map 的某个键」则不安全——需要那种隔离的调用方得自己再深拷一层。
//
// RecordsIndex 不复制：它是 KeyField 的惰性索引，键指向的是原记录指针，照搬会让副本的
// 索引指到别人的记录上。留空由 RecordByKey/Keys 在副本上按需重建。
func (self *TDataSet) Clone() *TDataSet {
	if self == nil {
		return nil
	}

	self.RLock()
	defer self.RUnlock()

	dst := &TDataSet{
		Name:     self.Name,
		KeyField: self.KeyField,
		classic:  self.classic,
	}
	newConfig(dst)
	if self.config != nil {
		dst.config.checkFields = self.config.checkFields
	}

	if self.fields != nil {
		dst.fields = make([]string, len(self.fields))
		copy(dst.fields, self.fields)
	}
	if self.fieldsIndex != nil {
		dst.fieldsIndex = make(map[string]int, len(self.fieldsIndex))
		for k, v := range self.fieldsIndex {
			dst.fieldsIndex[k] = v
		}
	}
	dst.FieldCount = self.FieldCount

	// 格式化器函数本身是无状态的纯转换，共享函数值即可；复制的是 map，
	// 使副本上的 SetFieldFormater 不会写回原数据集。
	if self.fieldFormater != nil {
		dst.fieldFormater = make(map[string]func(any) any, len(self.fieldFormater))
		for k, v := range self.fieldFormater {
			dst.fieldFormater[k] = v
		}
	}

	dst.Data = make([]*TRecordSet, 0, len(self.Data))
	for _, rec := range self.Data {
		if rec == nil {
			continue
		}
		dst.Data = append(dst.Data, rec.cloneInto(dst, len(dst.Data)))
	}

	dst.position.Store(self.position.Load())
	return dst
}

// Clone 返回一条**脱离**原数据集的记录副本：它自带字段索引，不再随原数据集的字段表变化，
// 也不会在 SetByField 时被追加回任何数据集。
//
// 值的复制语义与 TDataSet.Clone 一致（复合值仍共享底层数据）。
func (self *TRecordSet) Clone() *TRecordSet {
	if self == nil {
		return nil
	}

	dst := &TRecordSet{
		fieldsCount: self.fieldsCount,
		index:       -1, // 不隶属任何数据集
	}
	dst.values = copyValues(self.values)
	dst.ClassicValues = copyValues(self.ClassicValues)

	// 独立记录必须把字段索引落实下来：getFieldsIndex 在 fieldsIndex 为 nil 时会回退到
	// 寄托的 dataset，而副本没有寄托，回退结果是 nil，字段就全查不到了。
	if src := self.getFieldsIndex(); src != nil {
		dst.fieldsIndex = make(map[string]int, len(src))
		for k, v := range src {
			dst.fieldsIndex[k] = v
		}
	}

	return dst
}

// cloneInto 复制本记录并把它挂到 ds 名下（沿用 ds 的字段索引，与 AppendRecord 后的
// 记录形态一致：fieldsIndex 为 nil，查字段时回退到 dataset）。
func (self *TRecordSet) cloneInto(ds *TDataSet, index int) *TRecordSet {
	dst := &TRecordSet{
		dataset:     ds,
		fieldsCount: self.fieldsCount,
		index:       index,
	}
	dst.values = copyValues(self.values)
	dst.ClassicValues = copyValues(self.ClassicValues)

	// 仅当记录自带索引(未并入 dataset 的独立记录)时才复制；否则保持 nil，
	// 让它跟着 ds.fieldsIndex 走。
	if self.fieldsIndex != nil {
		dst.fieldsIndex = make(map[string]int, len(self.fieldsIndex))
		for k, v := range self.fieldsIndex {
			dst.fieldsIndex[k] = v
		}
	}

	return dst
}

func copyValues(src []interface{}) []interface{} {
	if src == nil {
		return nil
	}
	dst := make([]interface{}, len(src))
	copy(dst, src)
	return dst
}
