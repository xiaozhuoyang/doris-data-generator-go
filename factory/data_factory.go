package factory

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"doris-data-generator-go/config"
)

type datetimeContext struct {
	Start         time.Time
	End           time.Time
	TotalSeconds  int64
	TimePoints    []time.Time
	PartitionMode bool
}

type DataFactory struct {
	config             config.GeneratorConfig
	fieldConfigs       map[string]config.FieldConfig
	counter            map[string]int
	logGenerator       LogGenerator
	rng                *rand.Rand
	sequenceOffset     int64
	timestampOffset    int64
	timestampBase      int64
	timestampRows      int64
	partitionDateRange []string
	datetimeCache      map[string]datetimeContext
}

const internalLogTypePrefix = "__dg_internal_log_type__"

var factorySeedCounter atomic.Int64

type generatedLog struct {
	Text    string
	LogType string
}

func LogTypeMetadataKey(fieldName string) string {
	return internalLogTypePrefix + fieldName
}

func NewDataFactory(cfg config.GeneratorConfig, fieldConfigs map[string]config.FieldConfig) *DataFactory {
	seed := time.Now().UnixNano() + factorySeedCounter.Add(1)*7919
	return &DataFactory{
		config:        cfg,
		fieldConfigs:  fieldConfigs,
		counter:       map[string]int{},
		logGenerator:  LogGenerator{},
		rng:           rand.New(rand.NewSource(seed)),
		datetimeCache: map[string]datetimeContext{},
	}
}

func (f *DataFactory) ResetCounters() {
	f.counter = map[string]int{}
	f.sequenceOffset = 0
	f.timestampOffset = 0
	f.timestampBase = 0
	f.timestampRows = 0
}

func (f *DataFactory) SetSequenceOffset(offset int64) {
	f.sequenceOffset = offset
}

func (f *DataFactory) SetTimestampOffset(offset int64) {
	f.timestampOffset = offset
}

func (f *DataFactory) SetTimestampWindow(baseOffset int64, rows int) {
	f.timestampBase = baseOffset
	f.timestampRows = int64(rows)
}

func (f *DataFactory) SetPartitionDateRange(dateRange []string) {
	f.partitionDateRange = dateRange
	f.datetimeCache = map[string]datetimeContext{}
}

func (f *DataFactory) GenerateValue(fieldName, fieldType string, fieldConfig config.FieldConfig, typeParams map[string]int) any {
	value := f.generateValue(fieldName, fieldType, fieldConfig, typeParams)
	if log, ok := value.(generatedLog); ok {
		return log.Text
	}
	return value
}

func (f *DataFactory) generateValue(fieldName, fieldType string, fieldConfig config.FieldConfig, typeParams map[string]int) any {
	normalized := f.normalizeFieldConfig(fieldName, fieldType, fieldConfig, typeParams)
	switch strings.ToUpper(fieldType) {
	case "INT", "BIGINT", "SMALLINT", "TINYINT":
		return f.generateInt(fieldName, normalized)
	case "DECIMAL":
		return f.generateDecimal(normalized)
	case "VARCHAR", "CHAR":
		if isLogField(fieldName) {
			return f.generateLog(normalized, time.Time{})
		}
		return f.generateString(normalized, 0)
	case "DATETIME", "TIMESTAMP", "DATE":
		return f.generateDatetime(normalized, fieldName)
	case "DOUBLE", "FLOAT":
		return f.generateDouble(normalized)
	case "BOOLEAN":
		return f.generateBoolean(normalized)
	default:
		return f.generateString(normalized, 0)
	}
}

func (f *DataFactory) GenerateBatch(columns []config.Column, numRows int) []map[string]any {
	rows := make([]map[string]any, 0, numRows)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		row := make(map[string]any, len(columns))
		for _, col := range columns {
			fieldConfig, ok := f.fieldConfigs[col.Name]
			if !ok {
				fieldConfig = config.NewFieldConfig(col.Type)
			}
			value := f.generateValue(col.Name, col.Type, fieldConfig, col.TypeParams)
			if log, ok := value.(generatedLog); ok {
				row[col.Name] = log.Text
				row[LogTypeMetadataKey(col.Name)] = log.LogType
				continue
			}
			row[col.Name] = value
		}
		rows = append(rows, row)
	}
	return rows
}

func (f *DataFactory) normalizeFieldConfig(fieldName, fieldType string, fieldConfig config.FieldConfig, typeParams map[string]int) config.FieldConfig {
	if fieldConfig.Type == "" {
		fieldConfig = config.NewFieldConfig(fieldType)
	}
	if fieldConfig.TypeParams == nil {
		fieldConfig.TypeParams = map[string]int{}
	}
	for key, value := range typeParams {
		if _, ok := fieldConfig.TypeParams[key]; !ok {
			fieldConfig.TypeParams[key] = value
		}
	}
	if fieldConfig.Length == nil {
		if length, ok := typeParams["length"]; ok {
			fieldConfig.Length = intPtr(length)
		}
	}
	if fieldConfig.ValuesWithWeights != nil && len(fieldConfig.WeightedValues) == 0 {
		fieldConfig.WeightedValues = make([]any, 0, len(fieldConfig.ValuesWithWeights))
		fieldConfig.WeightedWeights = make([]float64, 0, len(fieldConfig.ValuesWithWeights))
		for _, item := range fieldConfig.ValuesWithWeights {
			fieldConfig.WeightedValues = append(fieldConfig.WeightedValues, item.Value)
			fieldConfig.WeightedWeights = append(fieldConfig.WeightedWeights, item.Weight)
		}
	}
	if fieldConfig.LogTypes != nil && len(fieldConfig.LogTypeValues) == 0 {
		fieldConfig.LogTypeValues = make([]string, 0, len(fieldConfig.LogTypes))
		fieldConfig.LogTypeWeights = make([]float64, 0, len(fieldConfig.LogTypes))
		for _, item := range fieldConfig.LogTypes {
			fieldConfig.LogTypeValues = append(fieldConfig.LogTypeValues, item.Name)
			fieldConfig.LogTypeWeights = append(fieldConfig.LogTypeWeights, item.Weight)
		}
	}
	if isLogField(fieldName) && fieldConfig.LogType == "" && len(fieldConfig.LogTypeValues) == 0 {
		fieldConfig.LogType = "nginx_access"
	}
	f.fieldConfigs[fieldName] = fieldConfig
	return fieldConfig
}

func (f *DataFactory) generateInt(fieldName string, fieldConfig config.FieldConfig) any {
	if f.shouldBeNull(fieldConfig) {
		return nil
	}
	if len(fieldConfig.Range) == 2 {
		minValue := int(math.Round(fieldConfig.Range[0]))
		maxValue := int(math.Round(fieldConfig.Range[1]))
		return randInt(f.rng, minValue, maxValue)
	}
	return int(f.sequenceOffset) + f.nextCounter(fieldName)
}

func (f *DataFactory) generateDecimal(fieldConfig config.FieldConfig) any {
	if f.shouldBeNull(fieldConfig) {
		return nil
	}

	value := f.rng.Float64() * 1000000
	if len(fieldConfig.Range) == 2 {
		minValue := fieldConfig.Range[0]
		maxValue := fieldConfig.Range[1]
		value = minValue + f.rng.Float64()*(maxValue-minValue)
	}

	scale := 2
	if v, ok := fieldConfig.TypeParams["scale"]; ok {
		scale = v
	}
	return strconv.FormatFloat(value, 'f', scale, 64)
}

func (f *DataFactory) generateString(fieldConfig config.FieldConfig, length int) any {
	if f.shouldBeNull(fieldConfig) {
		return nil
	}

	targetLength := length
	if targetLength == 0 {
		if fieldConfig.Length != nil {
			targetLength = *fieldConfig.Length
		} else {
			targetLength = 32
		}
	}

	if len(fieldConfig.WeightedValues) > 0 {
		return chooseWeighted(f.rng, fieldConfig.WeightedValues, fieldConfig.WeightedWeights)
	}
	if fieldConfig.LowCardinality && len(fieldConfig.Values) > 0 {
		return fieldConfig.Values[f.rng.Intn(len(fieldConfig.Values))]
	}

	charPool := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	if fieldConfig.LowCardinality {
		charPool = []rune("abcdefghijklmnopqrst0123456789")
	}

	builder := strings.Builder{}
	for builder.Len() < targetLength {
		if fieldConfig.ChineseRatio > 0 && f.rng.Float64() < fieldConfig.ChineseRatio {
			if f.rng.Float64() < 0.3 {
				builder.WriteString(randomChoice(f.rng, chineseWords))
			} else {
				builder.WriteRune(randomChoice(f.rng, chineseChars))
			}
			continue
		}
		builder.WriteRune(randomChoice(f.rng, charPool))
	}

	result := []rune(builder.String())
	if len(result) > targetLength {
		result = result[:targetLength]
	}
	return string(result)
}

func (f *DataFactory) generateDatetime(fieldConfig config.FieldConfig, fieldName string) any {
	if f.shouldBeNull(fieldConfig) {
		return nil
	}

	context := f.getDatetimeContext(fieldName, fieldConfig)
	if len(context.TimePoints) > 0 {
		value := randomChoice(f.rng, context.TimePoints)
		if strings.EqualFold(fieldConfig.Type, "DATE") {
			return value.Format("2006-01-02")
		}
		return formatDatetimeValue(value, datetimeScale(fieldConfig))
	}

	value := f.sequentialDatetimeValue(context, datetimeScale(fieldConfig))

	if strings.EqualFold(fieldConfig.Type, "DATE") {
		return value.Format("2006-01-02")
	}
	return formatDatetimeValue(value, datetimeScale(fieldConfig))
}

func (f *DataFactory) sequentialDatetimeValue(context datetimeContext, scale int) time.Time {
	offset := f.timestampOffset
	f.timestampOffset++

	totalRows := int64(f.config.Rows)
	baseOffset := int64(0)
	if context.PartitionMode && f.timestampRows > 0 {
		totalRows = f.timestampRows
		baseOffset = f.timestampBase
	}
	if totalRows <= 1 {
		return context.Start
	}

	localOffset := offset - baseOffset
	if localOffset < 0 {
		localOffset = 0
	}
	if localOffset >= totalRows {
		localOffset = totalRows - 1
	}

	delta := context.End.Sub(context.Start)
	if delta <= 0 {
		return context.Start
	}
	step := float64(delta) / float64(totalRows-1)
	value := context.Start.Add(time.Duration(step * float64(localOffset)))
	if scale <= 0 {
		return value.Truncate(time.Second)
	}
	return value.Truncate(time.Microsecond)
}

func datetimeScale(fieldConfig config.FieldConfig) int {
	scale := fieldConfig.TypeParams["scale"]
	if scale < 0 {
		return 0
	}
	if scale > 6 {
		return 6
	}
	return scale
}

func randomFractionalDuration(rng *rand.Rand, scale int) time.Duration {
	if scale <= 0 {
		return 0
	}
	multiplier := int64(1)
	for idx := 0; idx < scale; idx++ {
		multiplier *= 10
	}
	fraction := rng.Int63n(multiplier)
	micros := fraction
	for idx := scale; idx < 6; idx++ {
		micros *= 10
	}
	return time.Duration(micros) * time.Microsecond
}

func formatDatetimeValue(value time.Time, scale int) string {
	if scale <= 0 {
		return value.Format("2006-01-02 15:04:05")
	}
	fraction := value.Nanosecond() / int(time.Microsecond)
	for idx := scale; idx < 6; idx++ {
		fraction /= 10
	}
	return fmt.Sprintf("%s.%0*d", value.Format("2006-01-02 15:04:05"), scale, fraction)
}

func (f *DataFactory) generateDouble(fieldConfig config.FieldConfig) any {
	if f.shouldBeNull(fieldConfig) {
		return nil
	}
	if len(fieldConfig.Range) == 2 {
		return fieldConfig.Range[0] + f.rng.Float64()*(fieldConfig.Range[1]-fieldConfig.Range[0])
	}
	return f.rng.Float64() * 1000000
}

func (f *DataFactory) generateBoolean(fieldConfig config.FieldConfig) any {
	if f.shouldBeNull(fieldConfig) {
		return nil
	}
	return f.rng.Intn(2) == 0
}

func (f *DataFactory) generateLog(fieldConfig config.FieldConfig, baseTime time.Time) any {
	if f.shouldBeNull(fieldConfig) {
		return nil
	}

	logType := fieldConfig.LogType
	if len(fieldConfig.LogTypeValues) > 0 {
		selected := chooseWeightedString(f.rng, fieldConfig.LogTypeValues, fieldConfig.LogTypeWeights)
		if selected != "" {
			logType = selected
		}
	}
	if logType == "" {
		logType = "nginx_access"
	}

	logLine := f.logGenerator.Generate(f.rng, logType, baseTime)
	if fieldConfig.ChineseRatio > 0 && f.rng.Float64() < fieldConfig.ChineseRatio {
		if IsJSONLogType(logType) {
			logLine = InjectChineseIntoJSONLog(f.rng, logLine)
		} else {
			logLine = InjectChineseFull(f.rng, logLine)
		}
	}
	return generatedLog{Text: logLine, LogType: logType}
}

func (f *DataFactory) getDatetimeContext(fieldName string, fieldConfig config.FieldConfig) datetimeContext {
	cacheKey := fmt.Sprintf("%s|%v|%v|%v", fieldName, fieldConfig.DateRange, f.config.DatetimeRange, f.partitionDateRange)
	if cached, ok := f.datetimeCache[cacheKey]; ok {
		return cached
	}

	var startStr, endStr string
	partitionMode := false

	switch {
	case len(f.partitionDateRange) == 2 && f.config.PartitionByDate == fieldName:
		startStr, endStr = f.partitionDateRange[0], f.partitionDateRange[1]
		partitionMode = true
	case len(fieldConfig.DateRange) == 2:
		startStr, endStr = fieldConfig.DateRange[0], fieldConfig.DateRange[1]
	case len(f.config.DatetimeRange) == 2:
		startStr, endStr = f.config.DatetimeRange[0], f.config.DatetimeRange[1]
	default:
		end := time.Now().Truncate(24 * time.Hour)
		start := end.AddDate(-1, 0, 0)
		context := buildDatetimeContext(start, end, fieldConfig.LowCardinality, false)
		f.datetimeCache[cacheKey] = context
		return context
	}

	start, err := parseDatetimeRangeBoundary(startStr, false)
	if err != nil {
		start = time.Now().AddDate(-1, 0, 0).Truncate(24 * time.Hour)
	}
	end, err := parseDatetimeRangeBoundary(endStr, true)
	if err != nil {
		end = start.Add(24 * time.Hour)
	}
	context := buildDatetimeContext(start, end, fieldConfig.LowCardinality, partitionMode)
	f.datetimeCache[cacheKey] = context
	return context
}

func parseDatetimeRangeBoundary(value string, isEnd bool) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	layouts := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
	}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, trimmed)
		if err != nil {
			continue
		}
		if isEnd && layout == "2006-01-02" {
			return parsed.Add(24*time.Hour - time.Microsecond), nil
		}
		return parsed, nil
	}
	return time.Time{}, fmt.Errorf("unsupported datetime range boundary: %s", value)
}

func buildDatetimeContext(start, end time.Time, lowCardinality, partitionMode bool) datetimeContext {
	totalSeconds := int64(end.Sub(start).Seconds())
	if totalSeconds <= 0 {
		totalSeconds = 86400
	}

	var timePoints []time.Time
	if lowCardinality {
		delta := end.Sub(start)
		timePoints = []time.Time{
			start,
			start.Add(delta / 4),
			start.Add(delta / 2),
			start.Add(delta * 3 / 4),
			end,
		}
	}

	return datetimeContext{
		Start:         start,
		End:           end,
		TotalSeconds:  totalSeconds,
		TimePoints:    timePoints,
		PartitionMode: partitionMode,
	}
}

func (f *DataFactory) nextCounter(fieldName string) int {
	f.counter[fieldName]++
	return f.counter[fieldName]
}

func (f *DataFactory) shouldBeNull(fieldConfig config.FieldConfig) bool {
	if fieldConfig.NullRatio <= 0 {
		return false
	}
	if fieldConfig.NullRatio >= 1 {
		return true
	}
	return f.rng.Float64() < fieldConfig.NullRatio
}

func chooseWeighted(rng *rand.Rand, values []any, weights []float64) any {
	if len(values) == 0 || len(values) != len(weights) {
		return nil
	}
	total := 0.0
	for _, weight := range weights {
		total += weight
	}
	if total <= 0 {
		return values[0]
	}
	threshold := rng.Float64() * total
	sum := 0.0
	for idx, weight := range weights {
		sum += weight
		if threshold <= sum {
			return values[idx]
		}
	}
	return values[len(values)-1]
}

func chooseWeightedString(rng *rand.Rand, values []string, weights []float64) string {
	selected := chooseWeighted(rng, toAnySlice(values), weights)
	if selected == nil {
		return ""
	}
	return fmt.Sprintf("%v", selected)
}

func toAnySlice(values []string) []any {
	result := make([]any, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

func isLogField(fieldName string) bool {
	switch strings.ToLower(fieldName) {
	case "msg", "message", "log", "log_message":
		return true
	default:
		return false
	}
}

func intPtr(value int) *int {
	return &value
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
