package app

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"doris-data-generator-go/config"
	"doris-data-generator-go/ddlparser"
	"doris-data-generator-go/factory"
	"doris-data-generator-go/writer"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

type Options struct {
	Version           bool
	DDL               string
	DDLFile           string
	SourceTable       string
	TargetTable       string
	Cluster           string
	CopyMode          string
	Host              string
	Port              int
	User              string
	Password          string
	LogFile           string
	Resume            bool
	DryRun            bool
	RemapField        string
	RemapSrc          string
	RemapDst          string
	RemapStringField  string
	RemapStringValues string
	TVFImport         bool
	TVFBatchFiles     int
	TVFLogType        string
	TVFRemapString    string
	TVFRemapValues    string
	TVFCluster        string
	SkipSchemaCheck   bool
	Demo              string
	DemoFile          string
	ConfigFile        string
	Rows              int
	Partitions        int
	FileSize          string
	Output            string
	OSSBucket         string
	OSSPath           string
	OSSEndpoint       string
	OSSAK             string
	OSSSK             string
	OSSAddressing     string
	Config            string
	BatchSize         int
	Compression       string
	Parallel          int
	WriterParallel    int
	UploadParallel    int
	PipelineBuffer    int
	ChunkSize         int
	DorisHost         string
	DorisPort         int
	DorisDatabase     string
	DorisTable        string
	DorisUser         string
	DorisPassword     string
	DorisBatchSize    int
	GroupCommit       bool
	NoUpload          bool
	NoParquet         bool
	Cleanup           bool
}

func Run(args []string) error {
	options, err := parseArgs(args)
	if err != nil {
		return err
	}

	if options.Version {
		fmt.Printf("doris-data-generator-go version=%s commit=%s build_time=%s runtime=%s/%s\n", Version, GitCommit, BuildTime, runtime.GOOS, runtime.GOARCH)
		return nil
	}

	if isCopyMode(options) {
		return runDorisCopy(options)
	}
	if options.TVFImport {
		return runTVFImport(options)
	}

	columns, err := parseDDL(options)
	if err != nil {
		return err
	}
	fmt.Printf("Parsed %d columns:\n", len(columns))
	for _, col := range columns {
		fmt.Printf("  - %s: %s\n", col.Name, col.Type)
	}

	if options.Demo != "" || options.DemoFile != "" {
		fmt.Println("Warning: --demo and --demo-file are ignored; use --config or --config-file to control field generation.")
	}

	configJSON, err := loadConfigJSON(options)
	if err != nil {
		return err
	}

	genConfig := buildGeneratorConfig(options, configJSON)
	fieldConfigs := buildFieldConfigs(columns, configJSON)

	dorisEnabled := options.DorisHost != "" && options.DorisDatabase != "" && options.DorisTable != ""
	ossEnabled := !options.NoUpload && options.OSSBucket != "" && options.OSSEndpoint != "" && options.OSSAK != "" && options.OSSSK != ""
	var dorisWriter *writer.DorisWriter
	if dorisEnabled {
		dorisWriter = writer.NewDorisWriter(
			options.DorisHost,
			options.DorisPort,
			options.DorisDatabase,
			options.DorisTable,
			options.DorisUser,
			options.DorisPassword,
			time.Hour,
			options.GroupCommit,
		)
	}

	var ossWriter *writer.OSSWriter
	if ossEnabled {
		ossWriter = writer.NewOSSWriter(
			options.OSSBucket,
			options.OSSPath,
			options.OSSEndpoint,
			options.OSSAK,
			options.OSSSK,
			options.OSSAddressing,
			time.Hour,
		)
	}

	numPartitions, rowsPerFile, partitionDates, err := planPartitions(options, genConfig, columns)
	if err != nil {
		return err
	}

	baseFilename := fmt.Sprintf("data_%s", time.Now().Format("20060102_150405"))
	start := time.Now()

	var parquetWriter *writer.ParquetWriter
	if !options.NoParquet {
		parquetWriter, err = writer.NewParquetWriter(genConfig.OutputDir, columns, genConfig.Compression)
		if err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
	}

	if options.NoParquet && !dorisEnabled {
		return fmt.Errorf("--no-parquet requires Doris output in the current Go rewrite")
	}

	if dorisWriter != nil && options.NoParquet {
		fmt.Printf("Using %d parallel workers, Doris batch size %d\n", maxInt(1, options.Parallel), maxInt(1, options.DorisBatchSize))
		if err := runDorisOnlyStreaming(
			options,
			columns,
			genConfig,
			fieldConfigs,
			numPartitions,
			rowsPerFile,
			partitionDates,
			dorisWriter,
			start,
		); err != nil {
			return err
		}
		fmt.Println("\nDone!")
		return nil
	}

	if parquetWriter != nil {
		if err := runChunkedParquetOutput(
			options,
			columns,
			genConfig,
			fieldConfigs,
			parquetWriter,
			ossWriter,
			dorisWriter,
			baseFilename,
			start,
		); err != nil {
			return err
		}
		fmt.Println("\nDone!")
		return nil
	}

	fmt.Println("\nDone!")
	return nil
}

func isCopyMode(options Options) bool {
	return options.SourceTable != "" || options.TargetTable != "" || options.CopyMode != ""
}

func uploadFilesToOSS(ossWriter *writer.OSSWriter, filenames []string, cleanup bool) error {
	for _, filename := range filenames {
		objectKey, err := ossWriter.UploadFile(filename)
		if err != nil {
			return fmt.Errorf("upload %s to OSS: %w", filename, err)
		}
		fmt.Printf("Uploaded to OSS: %s\n", objectKey)
		if cleanup {
			if err := os.Remove(filename); err != nil {
				return fmt.Errorf("cleanup local file %s: %w", filename, err)
			}
		}
	}
	return nil
}

func runChunkedParquetOutput(
	options Options,
	columns []config.Column,
	genConfig config.GeneratorConfig,
	fieldConfigs map[string]config.FieldConfig,
	parquetWriter *writer.ParquetWriter,
	ossWriter *writer.OSSWriter,
	dorisWriter *writer.DorisWriter,
	baseFilename string,
	start time.Time,
) error {
	outputChunkSize := maxInt(1, options.ChunkSize)
	generationBatchSize := maxInt(1, options.BatchSize)
	parallelism := maxInt(1, options.Parallel)
	writerParallelism := resolveWriterParallelism(options.WriterParallel, parallelism)
	uploadParallelism := resolveUploadParallelism(options.UploadParallel, parallelism)
	pipelineBuffer := maxInt(1, options.PipelineBuffer)
	fmt.Printf(
		"Using %d generation workers, %d parquet writer workers, %d upload workers, pipeline buffer %d, output chunk size %d, generation batch size %d\n",
		parallelism,
		writerParallelism,
		uploadParallelism,
		pipelineBuffer,
		outputChunkSize,
		generationBatchSize,
	)
	totalGenerated := 0
	chunkIdx := 0
	var uploads *asyncUploader
	if ossWriter != nil {
		uploads = newAsyncUploader(ossWriter, options.Cleanup, uploadParallelism)
		defer uploads.close()
	}

	writeTasks := make(chan parquetWriteTask, pipelineBuffer)
	errCh := make(chan error, writerParallelism+1)
	var writers sync.WaitGroup
	for writerIdx := 0; writerIdx < writerParallelism; writerIdx++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for task := range writeTasks {
				filenames, writeErr := writePartitionRows(parquetWriter, task.rows, baseFilename, task.chunkIdx, fieldConfigs)
				if writeErr != nil {
					sendPipelineError(errCh, fmt.Errorf("write output file: %w", writeErr))
					return
				}
				for _, filename := range filenames {
					fmt.Printf("\nWritten parquet file: %s\n", filename)
				}
				if uploads != nil {
					if err := uploads.enqueue(filenames); err != nil {
						sendPipelineError(errCh, err)
						return
					}
				}
				if dorisWriter != nil {
					if err := writeToDorisInBatches(dorisWriter, task.rows, options.DorisBatchSize); err != nil {
						sendPipelineError(errCh, err)
						return
					}
				}
			}
		}()
	}

	for totalGenerated < options.Rows {
		if err := pollPipelineError(errCh); err != nil {
			close(writeTasks)
			writers.Wait()
			return err
		}

		batchRows := minInt(outputChunkSize, options.Rows-totalGenerated)
		rows, err := generatePartitionRowsParallel(
			columns,
			genConfig,
			fieldConfigs,
			chunkIdx,
			batchRows,
			int64(totalGenerated),
			nil,
			parallelism,
			generationBatchSize,
		)
		if err != nil {
			close(writeTasks)
			writers.Wait()
			return err
		}

		totalGenerated += len(rows)
		printProgress(totalGenerated, options.Rows, start)

		task := parquetWriteTask{chunkIdx: chunkIdx, rows: rows}
		select {
		case writeTasks <- task:
		case err := <-errCh:
			close(writeTasks)
			writers.Wait()
			return err
		}
		chunkIdx++
	}
	close(writeTasks)
	writers.Wait()
	if err := pollPipelineError(errCh); err != nil {
		return err
	}
	if uploads != nil {
		return uploads.wait()
	}
	return nil
}

type parquetWriteTask struct {
	chunkIdx int
	rows     []map[string]any
}

func resolveWriterParallelism(configured int, generationParallelism int) int {
	if configured > 0 {
		return configured
	}
	return minInt(4, maxInt(1, generationParallelism/2))
}

func resolveUploadParallelism(configured int, generationParallelism int) int {
	if configured > 0 {
		return configured
	}
	return generationParallelism
}

func sendPipelineError(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func pollPipelineError(errCh <-chan error) error {
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

type asyncUploader struct {
	ossWriter *writer.OSSWriter
	cleanup   bool
	queue     chan string
	done      chan struct{}
	wg        sync.WaitGroup
	errOnce   sync.Once
	err       error
}

func newAsyncUploader(ossWriter *writer.OSSWriter, cleanup bool, workers int) *asyncUploader {
	uploader := &asyncUploader{
		ossWriter: ossWriter,
		cleanup:   cleanup,
		queue:     make(chan string, workers*2),
		done:      make(chan struct{}),
	}
	for idx := 0; idx < workers; idx++ {
		uploader.wg.Add(1)
		go uploader.run()
	}
	return uploader
}

func (u *asyncUploader) enqueue(filenames []string) error {
	if err := u.currentError(); err != nil {
		return err
	}
	for _, filename := range filenames {
		select {
		case <-u.done:
			if err := u.currentError(); err != nil {
				return err
			}
			return fmt.Errorf("upload worker stopped before enqueueing %s", filename)
		case u.queue <- filename:
		}
	}
	return nil
}

func (u *asyncUploader) run() {
	defer u.wg.Done()
	for filename := range u.queue {
		objectKey, err := u.ossWriter.UploadFile(filename)
		if err != nil {
			u.setError(fmt.Errorf("upload %s to OSS: %w", filename, err))
			continue
		}
		fmt.Printf("Uploaded to OSS: %s\n", objectKey)
		if u.cleanup {
			if err := os.Remove(filename); err != nil {
				u.setError(fmt.Errorf("cleanup local file %s: %w", filename, err))
			}
		}
	}
}

func (u *asyncUploader) wait() error {
	close(u.queue)
	u.wg.Wait()
	close(u.done)
	return u.currentError()
}

func (u *asyncUploader) close() {
	select {
	case <-u.done:
	default:
		close(u.done)
	}
}

func (u *asyncUploader) setError(err error) {
	u.errOnce.Do(func() {
		u.err = err
	})
}

func (u *asyncUploader) currentError() error {
	return u.err
}

func parseArgs(args []string) (Options, error) {
	fs := flag.NewFlagSet("doris-data-generator-go", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	options := Options{}
	fs.BoolVar(&options.Version, "version", false, "Print version information")
	fs.StringVar(&options.SourceTable, "source-table", "", "Source table for Doris copy mode")
	fs.StringVar(&options.SourceTable, "a", "", "Source table for Doris copy mode")
	fs.StringVar(&options.TargetTable, "target-table", "", "Target table for Doris copy mode")
	fs.StringVar(&options.TargetTable, "b", "", "Target table for Doris copy mode")
	fs.StringVar(&options.Cluster, "cluster", "", "Doris cluster name for copy mode")
	fs.StringVar(&options.CopyMode, "copy-mode", "", "Copy mode: partition or tablet")
	fs.StringVar(&options.CopyMode, "mode", "", "Copy mode: partition or tablet")
	fs.StringVar(&options.Host, "host", "127.0.0.1", "Doris host for copy mode")
	fs.StringVar(&options.Host, "h", "127.0.0.1", "Doris host for copy mode")
	fs.IntVar(&options.Port, "port", 9030, "Doris port for copy mode")
	fs.IntVar(&options.Port, "P", 9030, "Doris port for copy mode")
	fs.StringVar(&options.User, "user", "root", "Doris username for copy mode")
	fs.StringVar(&options.User, "u", "root", "Doris username for copy mode")
	fs.StringVar(&options.Password, "password", "", "Doris password for copy mode")
	fs.StringVar(&options.Password, "p", "", "Doris password for copy mode")
	fs.StringVar(&options.DorisDatabase, "database", "", "Doris database name")
	fs.StringVar(&options.DorisDatabase, "d", "", "Doris database name")
	fs.StringVar(&options.LogFile, "log-file", "partition_copy.log", "Copy mode log file path")
	fs.StringVar(&options.LogFile, "l", "partition_copy.log", "Copy mode log file path")
	fs.BoolVar(&options.Resume, "resume", false, "Resume copy mode from log")
	fs.BoolVar(&options.DryRun, "dry-run", false, "Preview copy SQL without executing")
	fs.StringVar(&options.RemapField, "remap-ctime", "", "Remap a time field in copy mode")
	fs.StringVar(&options.RemapSrc, "remap-src", "", "Original time range: start,end")
	fs.StringVar(&options.RemapDst, "remap-dst", "", "Target time range: start,end")
	fs.StringVar(&options.RemapStringField, "remap-string", "", "Remap a string field to int in copy mode")
	fs.StringVar(&options.RemapStringValues, "remap-string-values", "", "Explicit string mapping: a:1,b:2")
	fs.BoolVar(&options.TVFImport, "tvf-import", false, "Import parquet files from S3/OSS into Doris via S3 TVF")
	fs.IntVar(&options.TVFBatchFiles, "batch-files", 10, "Number of parquet files per TVF batch")
	fs.StringVar(&options.TVFLogType, "tvf-log-type", "", "Only import parquet files for one generated log type suffix, e.g. nginx_access")
	fs.StringVar(&options.TVFRemapString, "tvf-remap-string", "", "Remap one TVF source string field to int by CASE expression")
	fs.StringVar(&options.TVFRemapValues, "tvf-remap-string-values", "", "Explicit TVF string mapping: a:1,b:2")
	fs.StringVar(&options.TVFCluster, "tvf-cluster", "", "Doris cluster name for TVF import; runs USE @cluster before each insert")
	fs.BoolVar(&options.SkipSchemaCheck, "skip-schema-check", true, "Skip parquet schema validation for TVF import")
	fs.StringVar(&options.DDL, "ddl", "", "CREATE TABLE DDL statement")
	fs.StringVar(&options.DDLFile, "ddl-file", "", "Path to SQL file containing CREATE TABLE statement")
	fs.StringVar(&options.Demo, "demo", "", "Demo data in CSV format or JSON object")
	fs.StringVar(&options.DemoFile, "demo-file", "", "Path to CSV or JSON demo file")
	fs.StringVar(&options.ConfigFile, "config-file", "", "Path to JSON configuration file")
	fs.IntVar(&options.Rows, "rows", 1000000, "Total number of rows to generate")
	fs.IntVar(&options.Partitions, "partitions", 10, "Number of output partitions")
	fs.StringVar(&options.FileSize, "file-size", "", "Target file size, e.g. 128MB or 1GB")
	fs.StringVar(&options.Output, "output", "./data", "Output directory")
	fs.StringVar(&options.OSSBucket, "oss-bucket", "", "OSS bucket name")
	fs.StringVar(&options.OSSPath, "oss-path", "/data/", "OSS upload path")
	fs.StringVar(&options.OSSEndpoint, "oss-endpoint", "", "OSS endpoint")
	fs.StringVar(&options.OSSAK, "oss-ak", "", "OSS access key id")
	fs.StringVar(&options.OSSSK, "oss-sk", "", "OSS access key secret")
	fs.StringVar(&options.OSSAddressing, "oss-addressing-style", "auto", "S3 addressing style")
	fs.StringVar(&options.Config, "config", "", "JSON configuration string")
	fs.IntVar(&options.BatchSize, "batch-size", 10000, "Batch size for data generation")
	fs.StringVar(&options.Compression, "compression", "snappy", "Compression mode")
	fs.IntVar(&options.Parallel, "parallel", 1, "Number of parallel workers")
	fs.IntVar(&options.WriterParallel, "writer-parallel", 0, "Number of parquet writer workers (default: min(4, parallel/2))")
	fs.IntVar(&options.UploadParallel, "upload-parallel", 0, "Number of OSS upload workers (default: parallel)")
	fs.IntVar(&options.PipelineBuffer, "pipeline-buffer", 2, "Number of generated chunks allowed to wait for parquet writers")
	fs.IntVar(&options.ChunkSize, "chunksize", 100000, "Rows per output chunk/file group")
	fs.StringVar(&options.DorisHost, "doris-host", "", "Doris FE host")
	fs.IntVar(&options.DorisPort, "doris-port", 8030, "Doris FE http port")
	fs.StringVar(&options.DorisDatabase, "doris-database", "", "Doris database name")
	fs.StringVar(&options.DorisTable, "doris-table", "", "Doris table name")
	fs.StringVar(&options.DorisUser, "doris-user", "root", "Doris username")
	fs.StringVar(&options.DorisPassword, "doris-password", "", "Doris password")
	fs.IntVar(&options.DorisBatchSize, "doris-batch-size", 10000, "Doris stream load batch size")
	fs.BoolVar(&options.GroupCommit, "group-commit", false, "Enable Doris group commit")
	fs.BoolVar(&options.NoUpload, "no-upload", false, "Disable upload even if configured")
	fs.BoolVar(&options.NoParquet, "no-parquet", false, "Skip parquet generation")
	fs.BoolVar(&options.Cleanup, "cleanup", false, "Delete local files after upload")

	if err := fs.Parse(args); err != nil {
		return options, err
	}
	if options.Version {
		return options, nil
	}

	if isCopyMode(options) {
		if options.SourceTable == "" || options.TargetTable == "" {
			return options, fmt.Errorf("--source-table and --target-table are required for copy mode")
		}
		if options.DorisDatabase == "" {
			return options, fmt.Errorf("--database or -d is required for copy mode")
		}
		if options.CopyMode == "" {
			options.CopyMode = "partition"
		}
		if options.CopyMode != "partition" && options.CopyMode != "tablet" {
			return options, fmt.Errorf("--copy-mode must be partition or tablet")
		}
		return options, nil
	}
	if options.TVFImport {
		if options.OSSBucket == "" || options.OSSPath == "" || options.OSSEndpoint == "" || options.OSSAK == "" || options.OSSSK == "" {
			return options, fmt.Errorf("--tvf-import requires --oss-bucket, --oss-path, --oss-endpoint, --oss-ak and --oss-sk")
		}
		if options.DorisHost == "" || options.DorisDatabase == "" || options.DorisTable == "" {
			return options, fmt.Errorf("--tvf-import requires --doris-host, --doris-database and --doris-table")
		}
		if options.TVFBatchFiles <= 0 {
			options.TVFBatchFiles = 10
		}
		if options.TVFRemapString == "" && options.RemapStringField != "" {
			options.TVFRemapString = options.RemapStringField
		}
		if options.TVFRemapValues == "" && options.RemapStringValues != "" {
			options.TVFRemapValues = options.RemapStringValues
		}
		if options.TVFRemapString != "" && options.TVFRemapValues == "" {
			return options, fmt.Errorf("--tvf-remap-string requires --tvf-remap-string-values")
		}
		if options.TVFCluster == "" && options.Cluster != "" {
			options.TVFCluster = options.Cluster
		}
		if options.TVFCluster != "" && escapeClusterName(options.TVFCluster) != strings.TrimSpace(options.TVFCluster) {
			return options, fmt.Errorf("--tvf-cluster contains invalid characters: %s", options.TVFCluster)
		}
		return options, nil
	}

	if (options.DDL == "") == (options.DDLFile == "") {
		return options, fmt.Errorf("exactly one of --ddl or --ddl-file is required")
	}
	if options.Demo != "" && options.DemoFile != "" {
		return options, fmt.Errorf("--demo and --demo-file are mutually exclusive")
	}
	if options.Config != "" && options.ConfigFile != "" {
		return options, fmt.Errorf("--config and --config-file are mutually exclusive")
	}
	if options.FileSize != "" && options.Partitions != 10 {
		return options, fmt.Errorf("--file-size and --partitions are mutually exclusive")
	}
	if options.Rows <= 0 {
		return options, fmt.Errorf("--rows must be greater than 0")
	}
	return options, nil
}

func parseDDL(options Options) ([]config.Column, error) {
	if options.DDLFile != "" {
		fmt.Printf("Reading DDL from file: %s\n", options.DDLFile)
		return ddlparser.ParseFile(options.DDLFile)
	}
	return ddlparser.Parse(options.DDL)
}

func parseDemo(options Options) (map[string]any, error) {
	if options.DemoFile != "" {
		fmt.Printf("Reading demo data from file: %s\n", options.DemoFile)
		return ddlparser.DemoToMapFile(options.DemoFile)
	}
	return ddlparser.DemoToMap(options.Demo)
}

func parseConfigJSON(configStr string) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(configStr), &payload); err != nil {
		return nil, fmt.Errorf("invalid JSON config: %w", err)
	}
	return payload, nil
}

func parseConfigJSONFile(filePath string) (map[string]any, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	return parseConfigJSON(string(content))
}

func loadConfigJSON(options Options) (map[string]any, error) {
	switch {
	case options.ConfigFile != "":
		fmt.Printf("Reading config from file: %s\n", options.ConfigFile)
		return parseConfigJSONFile(options.ConfigFile)
	case options.Config != "":
		return parseConfigJSON(options.Config)
	default:
		return map[string]any{}, nil
	}
}

func buildGeneratorConfig(options Options, configJSON map[string]any) config.GeneratorConfig {
	generatorConfig := config.NewGeneratorConfig()
	generatorConfig.Rows = options.Rows
	generatorConfig.Partitions = options.Partitions
	generatorConfig.OutputDir = options.Output
	generatorConfig.BatchSize = options.BatchSize
	generatorConfig.Compression = options.Compression
	generatorConfig.PartitionByDate = stringValue(configJSON["partition_by_date"])
	if rawRange, ok := configJSON["datetime_range"].([]any); ok {
		generatorConfig.DatetimeRange = toStringSlice(rawRange)
	}
	if lowCardinality, ok := configJSON["low_cardinality"].(map[string]any); ok {
		generatorConfig.LowCardinalityFields = lowCardinality
	}
	return generatorConfig
}

func buildFieldConfigs(columns []config.Column, configJSON map[string]any) map[string]config.FieldConfig {
	result := make(map[string]config.FieldConfig, len(columns))

	fieldsConfig, _ := configJSON["fields"].(map[string]any)
	globalLowCardinality, _ := configJSON["low_cardinality"].(map[string]any)
	globalDatetimeRange := toStringSliceFromAny(configJSON["datetime_range"])

	for _, column := range columns {
		fieldConfig := config.NewFieldConfig(column.Type)
		for key, value := range column.TypeParams {
			fieldConfig.TypeParams[key] = value
		}
		if length, ok := column.TypeParams["length"]; ok {
			fieldConfig.Length = intPtr(length)
		}

		if rawLowCardinality, ok := globalLowCardinality[column.Name]; ok {
			switch v := rawLowCardinality.(type) {
			case bool:
				fieldConfig.LowCardinality = v
			case map[string]any:
				fieldConfig.LowCardinality = boolValue(v["enabled"])
				fieldConfig.Values = toAnySliceFromAny(v["values"])
				fieldConfig.NullRatio = floatValue(v["null_ratio"])
			}
		}

		if len(globalDatetimeRange) == 2 && isDatetimeType(column.Type) {
			fieldConfig.DateRange = globalDatetimeRange
		}

		if rawField, ok := fieldsConfig[column.Name].(map[string]any); ok {
			applyFieldOverrides(&fieldConfig, rawField)
		}

		if isLogField(column.Name) && fieldConfig.LogType == "" && len(fieldConfig.LogTypeValues) == 0 {
			fieldConfig.LogType = "nginx_access"
		}
		result[column.Name] = fieldConfig
	}

	return result
}

func applyFieldOverrides(fieldConfig *config.FieldConfig, rawField map[string]any) {
	if value, ok := rawField["low_cardinality"]; ok {
		fieldConfig.LowCardinality = boolValue(value)
	}
	if value, ok := rawField["values"]; ok {
		switch values := value.(type) {
		case []any:
			if len(values) > 0 {
				if weighted, ok := values[0].([]any); ok && len(weighted) == 2 {
					fieldConfig.ValuesWithWeights = make([]config.WeightedValue, 0, len(values))
					for _, item := range values {
						pair, ok := item.([]any)
						if !ok || len(pair) != 2 {
							continue
						}
						fieldConfig.ValuesWithWeights = append(fieldConfig.ValuesWithWeights, config.WeightedValue{
							Value:  pair[0],
							Weight: floatValue(pair[1]),
						})
					}
					fieldConfig.LowCardinality = true
				} else {
					fieldConfig.Values = values
				}
			}
		}
	}
	if value, ok := rawField["length"]; ok {
		length := int(floatValue(value))
		fieldConfig.Length = &length
	}
	if value, ok := rawField["range"]; ok {
		fieldConfig.Range = toFloatSlice(value)
	}
	if value, ok := rawField["null_ratio"]; ok {
		fieldConfig.NullRatio = floatValue(value)
	}
	if value, ok := rawField["date_range"]; ok {
		fieldConfig.DateRange = toStringSliceFromAny(value)
	}
	if value, ok := rawField["log_type"]; ok {
		fieldConfig.LogType = stringValue(value)
	}
	if value, ok := rawField["log_types"]; ok {
		if items, ok := value.([]any); ok {
			fieldConfig.LogTypes = make([]config.WeightedLogType, 0, len(items))
			for _, item := range items {
				pair, ok := item.([]any)
				if !ok || len(pair) != 2 {
					continue
				}
				fieldConfig.LogTypes = append(fieldConfig.LogTypes, config.WeightedLogType{
					Name:   stringValue(pair[0]),
					Weight: floatValue(pair[1]),
				})
			}
		}
	}
	if value, ok := rawField["chinese_ratio"]; ok {
		fieldConfig.ChineseRatio = floatValue(value)
	}
}

func planPartitions(options Options, generatorConfig config.GeneratorConfig, columns []config.Column) (int, int, [][]string, error) {
	if generatorConfig.PartitionByDate != "" && len(generatorConfig.DatetimeRange) == 2 {
		startDate, err := parsePartitionDateBoundary(generatorConfig.DatetimeRange[0])
		if err != nil {
			return 0, 0, nil, err
		}
		endDate, err := parsePartitionDateBoundary(generatorConfig.DatetimeRange[1])
		if err != nil {
			return 0, 0, nil, err
		}
		startDate = startDate.Truncate(24 * time.Hour)
		endDate = endDate.Truncate(24 * time.Hour)
		if endDate.Before(startDate) {
			return 0, 0, nil, fmt.Errorf("datetime_range end date must be on or after start date")
		}

		totalDays := int(endDate.Sub(startDate).Hours()/24) + 1
		numPartitions := maxInt(1, options.Partitions)
		if numPartitions > totalDays {
			numPartitions = totalDays
		}

		rowsPerFile := maxInt(1, int(math.Ceil(float64(options.Rows)/float64(numPartitions))))
		partitionDates := make([][]string, 0, numPartitions)
		baseDaysPerPartition := totalDays / numPartitions
		remainderDays := totalDays % numPartitions
		partitionStart := startDate

		for idx := 0; idx < numPartitions; idx++ {
			spanDays := baseDaysPerPartition
			if idx < remainderDays {
				spanDays++
			}
			if spanDays <= 0 {
				spanDays = 1
			}

			partitionEnd := partitionStart.AddDate(0, 0, spanDays-1)
			partitionDates = append(partitionDates, []string{
				partitionStart.Format("2006-01-02"),
				partitionEnd.Format("2006-01-02"),
			})
			partitionStart = partitionEnd.AddDate(0, 0, 1)
		}
		return numPartitions, rowsPerFile, partitionDates, nil
	}

	if options.FileSize != "" {
		targetSize, err := parseFileSize(options.FileSize)
		if err != nil {
			return 0, 0, nil, err
		}
		rowSize := estimateRowSize(columns)
		rowsPerFile := maxInt(1, targetSize/rowSize)
		numPartitions := int(math.Ceil(float64(options.Rows) / float64(rowsPerFile)))
		return numPartitions, rowsPerFile, nil, nil
	}

	rowsPerFile := maxInt(1, options.Rows/options.Partitions)
	return options.Partitions, rowsPerFile, nil, nil
}

func parsePartitionDateBoundary(value string) (time.Time, error) {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
	} {
		if parsed, err := time.Parse(layout, strings.TrimSpace(value)); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported datetime_range value: %s", value)
}

func parseFileSize(sizeStr string) (int, error) {
	sizeStr = strings.TrimSpace(strings.ToUpper(sizeStr))
	units := map[string]int{
		"B":  1,
		"KB": 1024,
		"MB": 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
	}

	for _, unit := range []string{"GB", "MB", "KB", "B"} {
		if strings.HasSuffix(sizeStr, unit) {
			number := strings.TrimSpace(strings.TrimSuffix(sizeStr, unit))
			value, err := strconv.ParseFloat(number, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid file size: %s", sizeStr)
			}
			return int(value * float64(units[unit])), nil
		}
	}

	value, err := strconv.Atoi(sizeStr)
	if err != nil {
		return 0, fmt.Errorf("invalid file size: %s", sizeStr)
	}
	return value, nil
}

func estimateRowSize(columns []config.Column) int {
	total := 0
	for _, col := range columns {
		switch strings.ToUpper(col.Type) {
		case "BIGINT":
			total += 8
		case "INT", "INTEGER":
			total += 4
		case "SMALLINT":
			total += 2
		case "TINYINT", "BOOLEAN":
			total += 1
		case "DECIMAL":
			total += 16
		case "FLOAT", "DOUBLE", "DATETIME", "TIMESTAMP":
			total += 8
		case "DATE":
			total += 4
		case "VARCHAR", "CHAR":
			length := 64
			if v, ok := col.TypeParams["length"]; ok {
				length = v
			}
			total += length * 2
		default:
			total += 32
		}
	}
	return int(float64(total) * 1.2)
}

func writeToDorisInBatches(dorisWriter *writer.DorisWriter, rows []map[string]any, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 10000
	}
	for start := 0; start < len(rows); start += batchSize {
		end := minInt(len(rows), start+batchSize)
		result, err := dorisWriter.WriteBatch(stripInternalFields(rows[start:end]))
		if err != nil {
			return err
		}
		if status := stringValue(result["Status"]); status != "" && status != "Success" {
			return fmt.Errorf("doris stream load failed: %v", result)
		}
	}
	return nil
}

type partitionWriter interface {
	WritePartitioned(data []map[string]any, baseFilename string, partitionIdx int) (string, error)
	WritePartitionedWithSuffix(data []map[string]any, baseFilename string, partitionIdx int, suffix string) (string, error)
}

func writePartitionRows(outputWriter partitionWriter, rows []map[string]any, baseFilename string, partitionIdx int, fieldConfigs map[string]config.FieldConfig) ([]string, error) {
	splitField := logTypeSplitField(fieldConfigs)
	if splitField == "" {
		filename, err := outputWriter.WritePartitioned(stripInternalFields(rows), baseFilename, partitionIdx)
		if err != nil {
			return nil, err
		}
		return []string{filename}, nil
	}

	grouped := groupRowsByGeneratedLogType(rows, splitField)
	suffixes := make([]string, 0, len(grouped))
	for suffix := range grouped {
		suffixes = append(suffixes, suffix)
	}
	sort.Strings(suffixes)

	filenames := make([]string, 0, len(suffixes))
	for _, suffix := range suffixes {
		filename, err := outputWriter.WritePartitionedWithSuffix(stripInternalFields(grouped[suffix]), baseFilename, partitionIdx, suffix)
		if err != nil {
			return nil, err
		}
		filenames = append(filenames, filename)
	}
	return filenames, nil
}

func logTypeSplitField(fieldConfigs map[string]config.FieldConfig) string {
	fields := make([]string, 0, len(fieldConfigs))
	for fieldName, fieldConfig := range fieldConfigs {
		if !isLogField(fieldName) {
			continue
		}
		if len(fieldConfig.LogTypes) > 1 || len(fieldConfig.LogTypeValues) > 1 {
			fields = append(fields, fieldName)
		}
	}
	sort.Strings(fields)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func groupRowsByGeneratedLogType(rows []map[string]any, fieldName string) map[string][]map[string]any {
	metadataKey := factory.LogTypeMetadataKey(fieldName)
	grouped := map[string][]map[string]any{}
	for _, row := range rows {
		suffix := sanitizeFilenameSuffix(stringValue(row[metadataKey]))
		if suffix == "" {
			suffix = "unknown"
		}
		grouped[suffix] = append(grouped[suffix], row)
	}
	return grouped
}

func stripInternalFields(rows []map[string]any) []map[string]any {
	result := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		clean := make(map[string]any, len(row))
		for key, value := range row {
			if strings.HasPrefix(key, "__dg_internal_") {
				continue
			}
			clean[key] = value
		}
		result = append(result, clean)
	}
	return result
}

func sanitizeFilenameSuffix(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	builder := strings.Builder{}
	lastUnderscore := false
	for _, r := range value {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			builder.WriteRune(r)
			lastUnderscore = false
		case r == '_' || r == '-' || r == '.':
			if !lastUnderscore {
				builder.WriteRune('_')
				lastUnderscore = true
			}
		default:
			if !lastUnderscore {
				builder.WriteRune('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(builder.String(), "_")
}

func printProgress(current, total int, start time.Time) {
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	rate := float64(current) / elapsed
	pct := float64(current) * 100 / float64(total)
	fmt.Printf("\rProgress: %d/%d (%.2f%%) - %.0f rows/sec", current, total, pct, rate)
}

func floatValue(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		parsed, _ := v.Float64()
		return parsed
	default:
		return 0
	}
}

func boolValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	default:
		return false
	}
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toFloatSlice(value any) []float64 {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]float64, 0, len(items))
	for _, item := range items {
		result = append(result, floatValue(item))
	}
	return result
}

func toStringSlice(items []any) []string {
	result := make([]string, 0, len(items))
	for _, item := range items {
		result = append(result, stringValue(item))
	}
	return result
}

func toStringSliceFromAny(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	return toStringSlice(items)
}

func toAnySliceFromAny(value any) []any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	return items
}

func isDatetimeType(columnType string) bool {
	switch strings.ToUpper(columnType) {
	case "DATETIME", "TIMESTAMP", "DATE":
		return true
	default:
		return false
	}
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

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
