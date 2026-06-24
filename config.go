package dataset

type (
	Option func(*Config)
	Config struct {
		dataset     *TDataSet
		checkFields bool
	}
)

func newConfig(dataset *TDataSet, opts ...Option) *Config {
	cfg := &Config{
		dataset:     dataset,
		checkFields: false,
	}
	dataset.config = cfg
	cfg.Init(opts...)
	return cfg
}

func (self *Config) Init(opts ...Option) {
	for _, opt := range opts {
		if opt != nil {
			opt(self)
		}
	}
}

func WithData(datas ...map[string]any) Option {
	return func(cfg *Config) {
		for _, m := range datas {
			cfg.dataset.NewRecord(m)
		}
		cfg.dataset.First()
	}
}

func WithFieldsChecker() Option {
	return func(cfg *Config) {
		cfg.checkFields = true
	}
}

// WithFieldFormater 复制 src 数据集的 fieldFormater 到新数据集。
// 仅复制格式化器映射(逐项写入新 map,不共享底层 map),不复制数据。
// src 为 nil 或无格式化器时不做任何操作。
func WithFieldFormater(src *TDataSet) Option {
	return func(cfg *Config) {
		if src == nil {
			return
		}

		src.RLock()
		defer src.RUnlock()
		for name, format := range src.fieldFormater {
			cfg.dataset.SetFieldFormater(name, format)
		}
	}
}
