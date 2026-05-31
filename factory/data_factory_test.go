package factory

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"doris-data-generator-go/config"
)

func TestGenerateBatch(t *testing.T) {
	cfg := config.NewGeneratorConfig()
	fields := map[string]config.FieldConfig{
		"id":   config.NewFieldConfig("BIGINT"),
		"name": config.NewFieldConfig("VARCHAR"),
		"msg": func() config.FieldConfig {
			field := config.NewFieldConfig("VARCHAR")
			field.LogType = "nginx_access"
			return field
		}(),
	}
	length := 8
	nameField := fields["name"]
	nameField.Length = &length
	fields["name"] = nameField

	factory := NewDataFactory(cfg, fields)
	rows := factory.GenerateBatch([]config.Column{
		{Name: "id", Type: "BIGINT"},
		{Name: "name", Type: "VARCHAR", TypeParams: map[string]int{"length": 8}},
		{Name: "msg", Type: "VARCHAR"},
	}, 5)

	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
	if rows[0]["id"] == nil || rows[0]["name"] == nil || rows[0]["msg"] == nil {
		t.Fatalf("unexpected empty row: %#v", rows[0])
	}
}

func TestWeightedStringGeneration(t *testing.T) {
	cfg := config.NewGeneratorConfig()
	field := config.NewFieldConfig("VARCHAR")
	field.ValuesWithWeights = []config.WeightedValue{{Value: "active", Weight: 1}, {Value: "inactive", Weight: 0}}
	factory := NewDataFactory(cfg, map[string]config.FieldConfig{"status": field})

	value := factory.GenerateValue("status", "VARCHAR", field, nil)
	if value != "active" {
		t.Fatalf("expected active, got %v", value)
	}
}

func TestGenerateBatchAddsLogTypeMetadata(t *testing.T) {
	cfg := config.NewGeneratorConfig()
	field := config.NewFieldConfig("VARCHAR")
	field.LogTypes = []config.WeightedLogType{
		{Name: "nginx_access", Weight: 1},
		{Name: "json_log_large", Weight: 0},
	}
	factory := NewDataFactory(cfg, map[string]config.FieldConfig{"msg": field})

	rows := factory.GenerateBatch([]config.Column{{Name: "msg", Type: "VARCHAR"}}, 1)
	if rows[0]["msg"] == nil {
		t.Fatalf("expected msg value, got %#v", rows[0])
	}
	if rows[0][LogTypeMetadataKey("msg")] != "nginx_access" {
		t.Fatalf("expected log type metadata, got %#v", rows[0])
	}
}

func TestDatetimeScaleGeneratesMicroseconds(t *testing.T) {
	cfg := config.NewGeneratorConfig()
	cfg.DatetimeRange = []string{"2026-03-15 00:00:00", "2026-03-26 23:59:59"}
	field := config.NewFieldConfig("DATETIME")
	field.TypeParams["scale"] = 6
	factory := NewDataFactory(cfg, map[string]config.FieldConfig{"_ctime_": field})
	pattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$`)
	hasNonZeroMicros := false

	for idx := 0; idx < 20; idx++ {
		value := factory.GenerateValue("_ctime_", "DATETIME", field, nil)
		text, ok := value.(string)
		if !ok {
			t.Fatalf("expected string, got %#v", value)
		}
		if !pattern.MatchString(text) {
			t.Fatalf("expected datetime(6), got %q", text)
		}
		if !strings.HasSuffix(text, ".000000") {
			hasNonZeroMicros = true
		}
	}
	if !hasNonZeroMicros {
		t.Fatalf("expected at least one datetime value with non-zero microseconds")
	}
}

func TestDatetimeSequenceCoversConfiguredRange(t *testing.T) {
	cfg := config.NewGeneratorConfig()
	cfg.Rows = 3
	cfg.DatetimeRange = []string{"2026-03-25 00:00:00", "2026-03-26 23:59:59"}
	field := config.NewFieldConfig("DATETIME")
	factory := NewDataFactory(cfg, map[string]config.FieldConfig{"_ctime_": field})

	first := factory.GenerateValue("_ctime_", "DATETIME", field, nil)
	middle := factory.GenerateValue("_ctime_", "DATETIME", field, nil)
	last := factory.GenerateValue("_ctime_", "DATETIME", field, nil)

	if first != "2026-03-25 00:00:00" {
		t.Fatalf("unexpected first datetime: %#v", first)
	}
	if middle == first || middle == last {
		t.Fatalf("expected middle datetime between endpoints, got first=%#v middle=%#v last=%#v", first, middle, last)
	}
	if last != "2026-03-26 23:59:59" {
		t.Fatalf("unexpected last datetime: %#v", last)
	}
}

func TestDateOnlyDatetimeRangeEndIncludesWholeDay(t *testing.T) {
	end, err := parseDatetimeRangeBoundary("2026-03-26", true)
	if err != nil {
		t.Fatalf("parseDatetimeRangeBoundary returned error: %v", err)
	}
	if end.Format("2006-01-02 15:04:05.000000") != "2026-03-26 23:59:59.999999" {
		t.Fatalf("unexpected inclusive date end: %s", end.Format("2006-01-02 15:04:05.000000"))
	}
}

func TestDatetimeWithoutScaleKeepsSecondPrecision(t *testing.T) {
	cfg := config.NewGeneratorConfig()
	cfg.DatetimeRange = []string{"2026-03-15", "2026-03-26"}
	field := config.NewFieldConfig("DATETIME")
	factory := NewDataFactory(cfg, map[string]config.FieldConfig{"_ctime_": field})

	value := factory.GenerateValue("_ctime_", "DATETIME", field, nil)
	text, ok := value.(string)
	if !ok {
		t.Fatalf("expected string, got %#v", value)
	}
	if strings.Contains(text, ".") {
		t.Fatalf("expected second precision datetime, got %q", text)
	}
}

func TestJSONLogChineseInjectionKeepsValidJSON(t *testing.T) {
	cfg := config.NewGeneratorConfig()
	field := config.NewFieldConfig("VARCHAR")
	field.LogType = "json_log_large"
	field.ChineseRatio = 1
	factory := NewDataFactory(cfg, map[string]config.FieldConfig{"msg": field})

	for idx := 0; idx < 20; idx++ {
		value := factory.GenerateValue("msg", "VARCHAR", field, nil)
		text, ok := value.(string)
		if !ok {
			t.Fatalf("expected string, got %#v", value)
		}
		if !strings.HasPrefix(text, "{") {
			t.Fatalf("expected JSON object, got %q", text)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(text), &payload); err != nil {
			t.Fatalf("expected valid JSON, got error %v for %q", err, text)
		}
		if strings.HasPrefix(text, "处理 ") || strings.HasPrefix(text, "请求 ") {
			t.Fatalf("JSON log should not have a non-JSON prefix: %q", text)
		}
	}
}
