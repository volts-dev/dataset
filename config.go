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

func WithData(data []map[string]any) Option {
	return func(cfg *Config) {
		for _, m := range data {
			cfg.dataset.NewRecord(m)
		}
	}
}

func WithFieldsChecker() Option {
	return func(cfg *Config) {
		cfg.checkFields = true
	}
}
