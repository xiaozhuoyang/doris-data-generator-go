package config

type WeightedValue struct {
	Value  any
	Weight float64
}

type WeightedLogType struct {
	Name   string
	Weight float64
}

type FieldConfig struct {
	Type              string
	TypeParams        map[string]int
	LowCardinality    bool
	Length            *int
	Range             []float64
	DateRange         []string
	Format            string
	NullRatio         float64
	Values            []any
	ValuesWithWeights []WeightedValue
	WeightedValues    []any
	WeightedWeights   []float64
	LogType           string
	LogTypes          []WeightedLogType
	LogTypeValues     []string
	LogTypeWeights    []float64
	ChineseRatio      float64
}

func NewFieldConfig(fieldType string) FieldConfig {
	return FieldConfig{
		Type:       fieldType,
		TypeParams: map[string]int{},
		Format:     "2006-01-02 15:04:05",
	}
}

type GeneratorConfig struct {
	Rows                 int
	TotalSize            string
	Partitions           int
	OutputDir            string
	BatchSize            int
	Compression          string
	DatetimeRange        []string
	LowCardinalityFields map[string]any
	PartitionByDate      string
}

func NewGeneratorConfig() GeneratorConfig {
	return GeneratorConfig{
		Rows:                 1000000,
		Partitions:           10,
		OutputDir:            "./data",
		BatchSize:            10000,
		Compression:          "snappy",
		LowCardinalityFields: map[string]any{},
	}
}

type OSSConfig struct {
	Bucket          string
	Path            string
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	Enabled         bool
	AddressingStyle string
}

func NewOSSConfig() OSSConfig {
	return OSSConfig{
		Path:            "/",
		AddressingStyle: "auto",
	}
}

type Column struct {
	Name       string
	Type       string
	TypeParams map[string]int
	Nullable   bool
}
