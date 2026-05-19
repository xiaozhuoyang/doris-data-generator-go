package writer

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"doris-data-generator-go/config"

	parquet "github.com/parquet-go/parquet-go"
)

type ParquetWriter struct {
	outputDir   string
	columns     []config.Column
	compression string
	rowType     reflect.Type
	schema      *parquet.Schema
}

func NewParquetWriter(outputDir string, columns []config.Column, compression string) (*ParquetWriter, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, err
	}

	rowType := buildParquetRowType(columns, compression)
	schema := parquet.SchemaOf(reflect.New(rowType).Interface())
	return &ParquetWriter{
		outputDir:   outputDir,
		columns:     columns,
		compression: compression,
		rowType:     rowType,
		schema:      schema,
	}, nil
}

func (w *ParquetWriter) WritePartitioned(data []map[string]any, baseFilename string, partitionIdx int) (string, error) {
	filename := fmt.Sprintf("%s.%04d.parquet", baseFilename, partitionIdx)
	return w.writeRows(data, filename)
}

func (w *ParquetWriter) WritePartitionedWithSuffix(data []map[string]any, baseFilename string, partitionIdx int, suffix string) (string, error) {
	filename := fmt.Sprintf("%s.%04d.%s.parquet", baseFilename, partitionIdx, suffix)
	return w.writeRows(data, filename)
}

func (w *ParquetWriter) writeRows(data []map[string]any, filename string) (string, error) {
	outputPath := filepath.Join(w.outputDir, filename)
	file, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	rows, err := w.convertRows(data)
	if err != nil {
		return "", err
	}

	parquetWriter := parquet.NewGenericWriter[any](file, w.schema)
	if _, err := parquetWriter.Write(rows); err != nil {
		return "", err
	}
	if err := parquetWriter.Close(); err != nil {
		return "", err
	}
	return outputPath, nil
}

func (w *ParquetWriter) convertRows(data []map[string]any) ([]any, error) {
	rows := make([]any, 0, len(data))
	for rowIdx, row := range data {
		value := reflect.New(w.rowType).Elem()
		for colIdx, column := range w.columns {
			field := value.Field(colIdx)
			if !field.CanSet() {
				continue
			}
			raw := row[column.Name]
			if raw == nil {
				continue
			}
			converted, err := convertParquetValue(raw, column)
			if err != nil {
				return nil, fmt.Errorf("convert row %d column %s: %w", rowIdx, column.Name, err)
			}
			if !converted.IsValid() {
				continue
			}
			field.Set(converted)
		}
		rows = append(rows, value.Interface())
	}
	return rows, nil
}

func buildParquetRowType(columns []config.Column, compression string) reflect.Type {
	fields := make([]reflect.StructField, 0, len(columns))
	for idx, column := range columns {
		fieldType, tagOptions := parquetFieldTypeAndTag(column)
		tagParts := []string{column.Name, "optional"}
		if codec := parquetCompressionTag(compression); codec != "" {
			tagParts = append(tagParts, codec)
		}
		tagParts = append(tagParts, tagOptions...)

		fields = append(fields, reflect.StructField{
			Name: fmt.Sprintf("Column%d", idx),
			Type: fieldType,
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"%s"`, strings.Join(tagParts, ","))),
		})
	}
	return reflect.StructOf(fields)
}

func parquetFieldTypeAndTag(column config.Column) (reflect.Type, []string) {
	switch strings.ToUpper(column.Type) {
	case "BIGINT":
		return reflect.TypeOf(int64(0)), []string{"int(64)"}
	case "INT":
		return reflect.TypeOf(int32(0)), []string{"int(32)"}
	case "SMALLINT":
		return reflect.TypeOf(int32(0)), []string{"int(16)"}
	case "TINYINT":
		return reflect.TypeOf(int32(0)), []string{"int(8)"}
	case "DECIMAL":
		precision := column.TypeParams["precision"]
		scale := column.TypeParams["scale"]
		if precision <= 0 {
			precision = 18
		}
		return reflect.TypeOf(int64(0)), []string{fmt.Sprintf("decimal(%d:%d)", scale, precision)}
	case "DOUBLE":
		return reflect.TypeOf(float64(0)), nil
	case "FLOAT":
		return reflect.TypeOf(float32(0)), nil
	case "BOOLEAN":
		return reflect.TypeOf(false), nil
	case "DATE":
		return reflect.TypeOf(int32(0)), []string{"date"}
	case "DATETIME", "TIMESTAMP":
		return reflect.TypeOf(int64(0)), []string{"timestamp(microsecond:local)"}
	default:
		return reflect.TypeOf(""), nil
	}
}

func parquetCompressionTag(compression string) string {
	switch strings.ToLower(strings.TrimSpace(compression)) {
	case "", "snappy":
		return "snappy"
	case "gzip":
		return "gzip"
	case "brotli":
		return "brotli"
	case "lz4":
		return "lz4"
	case "zstd":
		return "zstd"
	case "none", "uncompressed":
		return "uncompressed"
	default:
		return "snappy"
	}
}

func convertParquetValue(value any, column config.Column) (reflect.Value, error) {
	switch strings.ToUpper(column.Type) {
	case "BIGINT":
		converted, err := toInt64(value)
		return pointerValue(converted), err
	case "INT", "SMALLINT", "TINYINT":
		converted, err := toInt32(value)
		return pointerValue(converted), err
	case "DECIMAL":
		converted, err := decimalToScaledInt64(value, column.TypeParams["scale"])
		return pointerValue(converted), err
	case "DOUBLE":
		converted, err := toFloat64(value)
		return pointerValue(converted), err
	case "FLOAT":
		converted, err := toFloat32(value)
		return pointerValue(converted), err
	case "BOOLEAN":
		converted, err := toBool(value)
		return pointerValue(converted), err
	case "DATE":
		converted, err := toDateDays(value)
		return pointerValue(converted), err
	case "DATETIME", "TIMESTAMP":
		converted, err := toTimestampMicros(value)
		return pointerValue(converted), err
	default:
		return pointerValue(fmt.Sprintf("%v", value)), nil
	}
}

func pointerValue[T any](value T) reflect.Value {
	return reflect.ValueOf(value)
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 value overflows int64: %d", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported int value %T", value)
	}
}

func toInt32(value any) (int32, error) {
	converted, err := toInt64(value)
	if err != nil {
		return 0, err
	}
	if converted < math.MinInt32 || converted > math.MaxInt32 {
		return 0, fmt.Errorf("value overflows int32: %d", converted)
	}
	return int32(converted), nil
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported float value %T", value)
	}
}

func toFloat32(value any) (float32, error) {
	converted, err := toFloat64(value)
	if err != nil {
		return 0, err
	}
	return float32(converted), nil
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return false, err
		}
		return parsed, nil
	default:
		return false, fmt.Errorf("unsupported bool value %T", value)
	}
}

func toTime(value any, defaultLayout string) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		trimmed := strings.TrimSpace(v)
		for _, layout := range []string{
			defaultLayout,
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05.999",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if parsed, err := time.Parse(layout, trimmed); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("unsupported time format: %s", v)
	default:
		return time.Time{}, fmt.Errorf("unsupported time value %T", value)
	}
}

func toDateDays(value any) (int32, error) {
	converted, err := toTime(value, "2006-01-02")
	if err != nil {
		return 0, err
	}
	return int32(converted.UTC().Truncate(24*time.Hour).Unix() / 86400), nil
}

func toTimestampMicros(value any) (int64, error) {
	converted, err := toTime(value, "2006-01-02 15:04:05")
	if err != nil {
		return 0, err
	}
	return converted.UnixMicro(), nil
}

func decimalToScaledInt64(value any, scale int) (int64, error) {
	converted, err := toFloat64(value)
	if err != nil {
		return 0, err
	}
	multiplier := math.Pow10(scale)
	return int64(math.Round(converted * multiplier)), nil
}
