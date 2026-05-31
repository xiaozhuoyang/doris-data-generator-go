package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"doris-data-generator-go/config"
	"doris-data-generator-go/factory"
	"doris-data-generator-go/writer"
)

func TestParseFileSize(t *testing.T) {
	value, err := parseFileSize("128MB")
	if err != nil {
		t.Fatalf("parseFileSize returned error: %v", err)
	}
	if value != 128*1024*1024 {
		t.Fatalf("unexpected parsed size: %d", value)
	}
}

func TestApplyTotalSizeRows(t *testing.T) {
	options := Options{TotalSize: "1KB"}
	err := applyTotalSizeRows(&options, []config.Column{
		{Name: "id", Type: "BIGINT"},
		{Name: "ts", Type: "DATETIME"},
	})
	if err != nil {
		t.Fatalf("applyTotalSizeRows returned error: %v", err)
	}
	if options.Rows != 54 {
		t.Fatalf("expected 54 rows, got %d", options.Rows)
	}
}

func TestBuildFieldConfigs(t *testing.T) {
	columns := []config.Column{
		{Name: "status", Type: "VARCHAR", TypeParams: map[string]int{"length": 16}},
		{Name: "created_at", Type: "DATETIME"},
	}
	configJSON := map[string]any{
		"datetime_range": []any{"2026-01-01", "2026-01-31"},
		"fields": map[string]any{
			"status": map[string]any{
				"values": []any{
					[]any{"active", 0.8},
					[]any{"inactive", 0.2},
				},
			},
		},
	}

	fieldConfigs := buildFieldConfigs(columns, configJSON)
	if len(fieldConfigs["status"].ValuesWithWeights) != 2 {
		t.Fatalf("expected weighted values, got %#v", fieldConfigs["status"])
	}
	if len(fieldConfigs["created_at"].DateRange) != 2 {
		t.Fatalf("expected inherited datetime range, got %#v", fieldConfigs["created_at"].DateRange)
	}
}

func TestBuildFieldConfigsSupportsSingleLogTypeString(t *testing.T) {
	columns := []config.Column{
		{Name: "msg", Type: "VARCHAR"},
	}
	configJSON := map[string]any{
		"fields": map[string]any{
			"msg": map[string]any{
				"log_types": "json_log_large",
			},
		},
	}

	fieldConfigs := buildFieldConfigs(columns, configJSON)
	if fieldConfigs["msg"].LogType != "json_log_large" {
		t.Fatalf("expected single log type string to map to LogType, got %#v", fieldConfigs["msg"])
	}
	if len(fieldConfigs["msg"].LogTypes) != 0 {
		t.Fatalf("expected no weighted log types for single string, got %#v", fieldConfigs["msg"].LogTypes)
	}
}

func TestGeneratePartitionRowsParallel(t *testing.T) {
	genConfig := config.NewGeneratorConfig()
	genConfig.DatetimeRange = []string{"2026-01-01", "2026-01-03"}

	length := 8
	fieldConfigs := map[string]config.FieldConfig{
		"id":   config.NewFieldConfig("BIGINT"),
		"name": config.NewFieldConfig("VARCHAR"),
	}
	nameField := fieldConfigs["name"]
	nameField.Length = &length
	fieldConfigs["name"] = nameField

	rows, err := generatePartitionRowsParallel(
		[]config.Column{
			{Name: "id", Type: "BIGINT"},
			{Name: "name", Type: "VARCHAR", TypeParams: map[string]int{"length": 8}},
			{Name: "created_at", Type: "DATETIME"},
		},
		genConfig,
		fieldConfigs,
		0,
		25,
		0,
		nil,
		4,
		5,
	)
	if err != nil {
		t.Fatalf("generatePartitionRowsParallel returned error: %v", err)
	}
	if len(rows) != 25 {
		t.Fatalf("expected 25 rows, got %d", len(rows))
	}
	firstID, ok := rows[0]["id"].(int)
	if !ok || firstID != 1 {
		t.Fatalf("expected first id to be 1, got %#v", rows[0]["id"])
	}
	sixthID, ok := rows[5]["id"].(int)
	if !ok || sixthID != 6 {
		t.Fatalf("expected first row of second chunk to continue at 6, got %#v", rows[5]["id"])
	}
	lastID, ok := rows[len(rows)-1]["id"].(int)
	if !ok || lastID != 25 {
		t.Fatalf("expected last id to be 25, got %#v", rows[len(rows)-1]["id"])
	}
	firstTime, ok := rows[0]["created_at"].(string)
	if !ok || firstTime == "" {
		t.Fatalf("expected created_at string, got %#v", rows[0]["created_at"])
	}
	lastTime, ok := rows[len(rows)-1]["created_at"].(string)
	if !ok || lastTime == "" || lastTime == firstTime {
		t.Fatalf("expected distinct created_at values across chunk offsets, got first=%#v last=%#v", firstTime, lastTime)
	}
}

func TestPlanPartitionsByDateRange(t *testing.T) {
	options := Options{
		Rows:       100,
		Partitions: 2,
	}
	genConfig := config.NewGeneratorConfig()
	genConfig.PartitionByDate = "created_at"
	genConfig.DatetimeRange = []string{"2026-01-01", "2026-01-04"}

	numPartitions, rowsPerFile, partitionDates, err := planPartitions(options, genConfig, nil)
	if err != nil {
		t.Fatalf("planPartitions returned error: %v", err)
	}
	if numPartitions != 2 {
		t.Fatalf("expected 2 partitions, got %d", numPartitions)
	}
	if rowsPerFile != 50 {
		t.Fatalf("expected 50 rows per file, got %d", rowsPerFile)
	}
	expected := [][]string{
		{"2026-01-01", "2026-01-02"},
		{"2026-01-03", "2026-01-04"},
	}
	for idx := range expected {
		if partitionDates[idx][0] != expected[idx][0] || partitionDates[idx][1] != expected[idx][1] {
			t.Fatalf("unexpected partition %d: got %#v want %#v", idx, partitionDates[idx], expected[idx])
		}
	}
}

func TestPlanPartitionsAcceptsDatetimeRangeWithTime(t *testing.T) {
	options := Options{
		Rows:       100,
		Partitions: 2,
	}
	genConfig := config.NewGeneratorConfig()
	genConfig.PartitionByDate = "_ctime_"
	genConfig.DatetimeRange = []string{"2026-03-25 00:00:00", "2026-03-26 23:59:59"}

	numPartitions, rowsPerFile, partitionDates, err := planPartitions(options, genConfig, nil)
	if err != nil {
		t.Fatalf("planPartitions returned error: %v", err)
	}
	if numPartitions != 2 {
		t.Fatalf("expected 2 partitions, got %d", numPartitions)
	}
	if rowsPerFile != 50 {
		t.Fatalf("expected 50 rows per file, got %d", rowsPerFile)
	}
	expected := [][]string{
		{"2026-03-25", "2026-03-25"},
		{"2026-03-26", "2026-03-26"},
	}
	for idx := range expected {
		if partitionDates[idx][0] != expected[idx][0] || partitionDates[idx][1] != expected[idx][1] {
			t.Fatalf("unexpected partition %d: got %#v want %#v", idx, partitionDates[idx], expected[idx])
		}
	}
}

func TestParseArgsAllowsMissingDemo(t *testing.T) {
	options, err := parseArgs([]string{
		"--ddl", "CREATE TABLE t1 (id BIGINT)",
		"--rows", "10",
	})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if options.Demo != "" || options.DemoFile != "" {
		t.Fatalf("expected demo inputs to remain empty, got %#v", options)
	}
}

func TestParseArgsConfigSourcesAreExclusive(t *testing.T) {
	_, err := parseArgs([]string{
		"--ddl", "CREATE TABLE t1 (id BIGINT)",
		"--config", `{"fields":{"id":{"range":[1,10]}}}`,
		"--config-file", "/tmp/test.json",
	})
	if err == nil {
		t.Fatalf("expected mutual exclusivity error for config sources")
	}
}

func TestTVFBuildSQLSupportsStringRemap(t *testing.T) {
	values, err := parseTVFRemapValues("matrix-agent-manager:10,otel-agent:20,o'clock:30")
	if err != nil {
		t.Fatalf("parseTVFRemapValues returned error: %v", err)
	}
	cfg := tvfImportConfig{
		accessKeyID:     "ak",
		accessKeySecret: "sk",
		bucket:          "bucket",
		remapString:     "app",
		remapValues:     values,
		cluster:         "doris_cluster",
	}

	sql := cfg.buildSQL([]string{"s3://bucket/path/data.parquet"}, []string{"_ctime_", "app", "msg"})
	expectedFragments := []string{
		"USE @doris_cluster;",
		"INSERT INTO ``.`` (`_ctime_`, `app`, `msg`)",
		"SELECT `_ctime_`, CASE",
		"WHEN `app` = 'matrix-agent-manager' THEN 10",
		"WHEN `app` = 'otel-agent' THEN 20",
		"WHEN `app` = 'o''clock' THEN 30",
		"ELSE 0 END, `msg` FROM s3(",
	}
	for _, fragment := range expectedFragments {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", fragment, sql)
		}
	}
}

func TestParseArgsTVFRemapReusesCopyRemapFlags(t *testing.T) {
	options, err := parseArgs([]string{
		"--tvf-import",
		"--oss-bucket", "bucket",
		"--oss-path", "/path/",
		"--oss-endpoint", "oss-cn-shanghai.aliyuncs.com",
		"--oss-ak", "ak",
		"--oss-sk", "sk",
		"--doris-host", "127.0.0.1",
		"--doris-database", "db",
		"--doris-table", "tbl",
		"--remap-string", "app",
		"--remap-string-values", "a:1,b:2",
	})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if options.TVFRemapString != "app" || options.TVFRemapValues != "a:1,b:2" {
		t.Fatalf("expected TVF remap to reuse copy remap flags, got %#v", options)
	}
}

func TestCopyIntStringRemapAcceptsStringIntPairs(t *testing.T) {
	cfg := copyConfig{
		remapIntStringField:  "app",
		remapIntStringValues: "matrix-agent-manager:100001,o'clock:100002",
	}
	if err := cfg.buildIntStringMap(); err != nil {
		t.Fatalf("buildIntStringMap returned error: %v", err)
	}
	sql := cfg.buildSelectExpr("app")
	expectedFragments := []string{
		"WHEN `app` = 100001 THEN 'matrix-agent-manager'",
		"WHEN `app` = 100002 THEN 'o''clock'",
		"ELSE CAST(`app` AS STRING) END",
	}
	for _, fragment := range expectedFragments {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", fragment, sql)
		}
	}
}

func TestParseArgsTVFClusterReusesClusterFlag(t *testing.T) {
	options, err := parseArgs([]string{
		"--tvf-import",
		"--oss-bucket", "bucket",
		"--oss-path", "/path/",
		"--oss-endpoint", "oss-cn-shanghai.aliyuncs.com",
		"--oss-ak", "ak",
		"--oss-sk", "sk",
		"--doris-host", "127.0.0.1",
		"--doris-database", "db",
		"--doris-table", "tbl",
		"--cluster", "doris_cluster",
	})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if options.TVFCluster != "doris_cluster" {
		t.Fatalf("expected TVFCluster to reuse --cluster, got %#v", options)
	}
}

func TestParseArgsTVFClusterRejectsUnsafeCharacters(t *testing.T) {
	_, err := parseArgs([]string{
		"--tvf-import",
		"--oss-bucket", "bucket",
		"--oss-path", "/path/",
		"--oss-endpoint", "oss-cn-shanghai.aliyuncs.com",
		"--oss-ak", "ak",
		"--oss-sk", "sk",
		"--doris-host", "127.0.0.1",
		"--doris-database", "db",
		"--doris-table", "tbl",
		"--tvf-cluster", "doris_cluster;drop",
	})
	if err == nil {
		t.Fatalf("expected unsafe cluster name error")
	}
}

func TestParseArgsSupportsStreamLoadParallel(t *testing.T) {
	options, err := parseArgs([]string{
		"--ddl", "CREATE TABLE t1 (id BIGINT)",
		"--doris-host", "127.0.0.1",
		"--doris-database", "db",
		"--doris-table", "tbl",
		"--no-parquet",
		"--stream-load-parallel", "8",
	})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if options.StreamLoadParallel != 8 {
		t.Fatalf("expected stream load parallel 8, got %#v", options)
	}
}

func TestParseArgsSupportsOrderedStreamLoad(t *testing.T) {
	options, err := parseArgs([]string{
		"--ddl", "CREATE TABLE t1 (id BIGINT)",
		"--doris-host", "127.0.0.1",
		"--doris-database", "db",
		"--doris-table", "tbl",
		"--no-parquet",
		"--ordered-stream-load",
	})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if !options.OrderedStreamLoad {
		t.Fatalf("expected ordered stream load, got %#v", options)
	}
}

func TestLoadConfigJSONFromFile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "generator.json")
	if err := os.WriteFile(configPath, []byte(`{"fields":{"status":{"values":["active","inactive"]}}}`), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	configJSON, err := loadConfigJSON(Options{ConfigFile: configPath})
	if err != nil {
		t.Fatalf("loadConfigJSON returned error: %v", err)
	}

	fields, ok := configJSON["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %#v", configJSON["fields"])
	}
	status, ok := fields["status"].(map[string]any)
	if !ok {
		t.Fatalf("expected status field config, got %#v", fields["status"])
	}
	values, ok := status["values"].([]any)
	if !ok || len(values) != 2 {
		t.Fatalf("expected values array from config file, got %#v", status["values"])
	}
}

func TestGroupRowsByGeneratedLogType(t *testing.T) {
	rows := []map[string]any{
		{"msg": "access", factory.LogTypeMetadataKey("msg"): "nginx_access"},
		{"msg": "json", factory.LogTypeMetadataKey("msg"): "json_log_large"},
		{"msg": "more access", factory.LogTypeMetadataKey("msg"): "nginx_access"},
	}

	grouped := groupRowsByGeneratedLogType(rows, "msg")
	if len(grouped["nginx_access"]) != 2 {
		t.Fatalf("expected two nginx_access rows, got %#v", grouped["nginx_access"])
	}
	if len(grouped["json_log_large"]) != 1 {
		t.Fatalf("expected one json_log_large row, got %#v", grouped["json_log_large"])
	}

	clean := stripInternalFields(grouped["nginx_access"])
	if _, ok := clean[0][factory.LogTypeMetadataKey("msg")]; ok {
		t.Fatalf("expected internal log type metadata to be stripped, got %#v", clean[0])
	}
}

func TestRunChunkedParquetOutputWritesByChunk(t *testing.T) {
	dir := t.TempDir()
	columns := []config.Column{
		{Name: "id", Type: "BIGINT"},
		{Name: "msg", Type: "VARCHAR"},
	}
	fieldConfigs := map[string]config.FieldConfig{
		"id": config.NewFieldConfig("BIGINT"),
		"msg": func() config.FieldConfig {
			field := config.NewFieldConfig("VARCHAR")
			field.LogTypes = []config.WeightedLogType{
				{Name: "nginx_access", Weight: 1},
				{Name: "json_log_large", Weight: 0},
			}
			return field
		}(),
	}
	parquetWriter, err := writer.NewParquetWriter(dir, columns, "snappy")
	if err != nil {
		t.Fatalf("NewParquetWriter returned error: %v", err)
	}

	options := Options{
		Rows:      25,
		ChunkSize: 10,
		Parallel:  2,
	}
	if err := runChunkedParquetOutput(
		options,
		columns,
		config.NewGeneratorConfig(),
		fieldConfigs,
		parquetWriter,
		nil,
		nil,
		"data_test",
		time.Now(),
	); err != nil {
		t.Fatalf("runChunkedParquetOutput returned error: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatalf("glob parquet files: %v", err)
	}
	if len(matches) != 3 {
		t.Fatalf("expected 3 chunk parquet files, got %d: %#v", len(matches), matches)
	}
}
