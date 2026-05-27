package app

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type copyPartitionInfo struct {
	name     string
	dataSize string
	rowCount int64
}

type copyTabletInfo struct {
	tabletID      int64
	partitionName string
	dataSize      string
	rowCount      int64
	backendID     int64
}

type copyResult struct {
	partitionName string
	success       bool
	durationMs    int64
	errorMsg      string
}

type copyJob struct {
	name string
	sql  string
}

type copyConfig struct {
	host                 string
	port                 int
	user                 string
	password             string
	database             string
	cluster              string
	sourceTable          string
	targetTable          string
	concurrency          int
	logFile              string
	dryRun               bool
	resume               bool
	mode                 string
	remapField           string
	remapSrcStart        int64
	remapSrcEnd          int64
	remapDstStart        int64
	remapDstEnd          int64
	remapStringField     string
	remapStringValues    string
	remapIntStringField  string
	remapIntStringValues string
	remapIsMs            bool
	remapColType         string
	remapStringColType   string
	stringMap            map[string]int
	intStringMap         map[int]string
}

func runDorisCopy(options Options) error {
	remapSrcStart, remapSrcEnd, err := parseCopyTimeRange(options.RemapSrc)
	if err != nil {
		return err
	}
	remapDstStart, remapDstEnd, err := parseCopyTimeRange(options.RemapDst)
	if err != nil {
		return err
	}
	if options.RemapField != "" {
		if remapSrcStart == 0 || remapSrcEnd == 0 || remapDstStart == 0 || remapDstEnd == 0 {
			return fmt.Errorf("--remap-ctime requires --remap-src and --remap-dst")
		}
		if remapSrcStart >= remapSrcEnd {
			return fmt.Errorf("--remap-src start must be before end")
		}
		if remapDstStart >= remapDstEnd {
			return fmt.Errorf("--remap-dst start must be before end")
		}
	}
	cfg := copyConfig{
		host:                 options.Host,
		port:                 options.Port,
		user:                 options.User,
		password:             options.Password,
		database:             options.DorisDatabase,
		cluster:              options.Cluster,
		sourceTable:          options.SourceTable,
		targetTable:          options.TargetTable,
		concurrency:          maxInt(1, options.Parallel),
		logFile:              options.LogFile,
		dryRun:               options.DryRun,
		resume:               options.Resume,
		mode:                 options.CopyMode,
		remapField:           options.RemapField,
		remapSrcStart:        remapSrcStart,
		remapSrcEnd:          remapSrcEnd,
		remapDstStart:        remapDstStart,
		remapDstEnd:          remapDstEnd,
		remapStringField:     options.RemapStringField,
		remapStringValues:    options.RemapStringValues,
		remapIntStringField:  options.RemapIntStringField,
		remapIntStringValues: options.RemapIntStringValues,
	}
	return cfg.run()
}

func (c *copyConfig) run() error {
	fmt.Println("============================================")
	fmt.Println("  Doris Copier")
	fmt.Println("============================================")
	fmt.Printf("Source:  %s.%s\n", c.database, c.sourceTable)
	fmt.Printf("Target:  %s.%s\n", c.database, c.targetTable)
	fmt.Printf("Host:    %s@%s:%d\n", c.user, c.host, c.port)
	if c.cluster != "" {
		fmt.Printf("Cluster: %s\n", c.cluster)
	}
	fmt.Printf("Concurrency: %d\n", c.concurrency)
	fmt.Printf("Log file: %s\n", c.logFile)

	if _, err := exec.LookPath("mysql"); err != nil {
		return fmt.Errorf("mysql client not found in PATH")
	}

	if err := c.testConnection(); err != nil {
		return err
	}
	columns, err := c.validateColumns()
	if err != nil {
		return err
	}
	if err := c.detectTimeUnit(); err != nil {
		return err
	}
	if err := c.buildStringMap(); err != nil {
		return err
	}
	if err := c.buildIntStringMap(); err != nil {
		return err
	}

	switch c.mode {
	case "tablet":
		return c.runTabletMode(columns)
	default:
		return c.runPartitionMode(columns)
	}
}

func (c *copyConfig) mysqlArgs() []string {
	args := []string{
		"-h", c.host,
		"-P", strconv.Itoa(c.port),
		"-u", c.user,
		"--protocol=tcp",
		"--default-character-set=utf8mb4",
		"--database=" + c.database,
	}
	if c.password != "" {
		args = append(args, "-p"+c.password)
	}
	return args
}

func (c *copyConfig) runMysqlQuery(query string) ([]string, error) {
	args := append(c.mysqlArgs(), "-N", "-B", "-e", query)
	cmd := exec.Command("mysql", args...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("mysql query failed: %s: %w", strings.TrimSpace(stderr.String()), err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(out.Bytes()))
	var lines []string
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text != "" {
			lines = append(lines, text)
		}
	}
	return lines, scanner.Err()
}

func (c *copyConfig) runMysqlTableQuery(query string) ([]string, [][]string, error) {
	args := append(c.mysqlArgs(), "-B", "-e", query)
	cmd := exec.Command("mysql", args...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, nil, fmt.Errorf("mysql query failed: %s: %w", strings.TrimSpace(stderr.String()), err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(out.Bytes()))
	var headers []string
	var rows [][]string
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		fields := strings.Split(text, "\t")
		if headers == nil {
			headers = fields
			continue
		}
		rows = append(rows, fields)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	if len(headers) == 0 {
		return nil, nil, fmt.Errorf("empty result for query: %s", query)
	}
	return headers, rows, nil
}

func (c *copyConfig) runMysqlExec(query string) error {
	args := append(c.mysqlArgs(), "-e", query)
	cmd := exec.Command("mysql", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysql exec failed: %s: %w", strings.TrimSpace(stderr.String()), err)
	}
	return nil
}

func (c *copyConfig) testConnection() error {
	lines, err := c.runMysqlQuery("SELECT 1")
	if err != nil {
		return err
	}
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "1" {
		return fmt.Errorf("connection test failed")
	}
	return nil
}

func (c *copyConfig) checkTableExists(table string) error {
	lines, err := c.runMysqlQuery("SHOW TABLES LIKE '" + table + "'")
	if err != nil {
		return err
	}
	if len(lines) == 0 {
		return fmt.Errorf("table not found: %s.%s", c.database, table)
	}
	return nil
}

func (c *copyConfig) getTableColumns(table string) ([]string, error) {
	lines, err := c.runMysqlQuery("DESC " + c.database + "." + table)
	if err != nil {
		return nil, err
	}
	var cols []string
	for _, line := range lines {
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		colType := strings.ToUpper(fields[1])
		cols = append(cols, name)
		if c.remapField != "" && name == c.remapField {
			c.remapColType = colType
		}
		if c.remapStringField != "" && name == c.remapStringField {
			c.remapStringColType = colType
		}
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found for %s", table)
	}
	return cols, nil
}

func (c *copyConfig) validateColumns() ([]string, error) {
	srcCols, err := c.getTableColumns(c.sourceTable)
	if err != nil {
		return nil, err
	}
	dstCols, err := c.getTableColumns(c.targetTable)
	if err != nil {
		return nil, err
	}
	if len(srcCols) != len(dstCols) {
		return nil, fmt.Errorf("column mismatch between %s and %s", c.sourceTable, c.targetTable)
	}
	return dstCols, nil
}

func (c *copyConfig) detectTimeUnit() error {
	if c.remapField == "" {
		return nil
	}
	if strings.HasPrefix(c.remapColType, "VARCHAR") || strings.HasPrefix(c.remapColType, "TEXT") || strings.HasPrefix(c.remapColType, "STRING") || strings.HasPrefix(c.remapColType, "CHAR") {
		return nil
	}
	lines, err := c.runMysqlQuery(fmt.Sprintf("SELECT `%s` FROM %s.%s WHERE `%s` IS NOT NULL AND `%s` > 0 LIMIT 1",
		c.remapField, c.database, c.sourceTable, c.remapField, c.remapField))
	if err != nil || len(lines) == 0 {
		return nil
	}
	val, _ := strconv.ParseInt(lines[0], 10, 64)
	c.remapIsMs = val > 1_000_000_000_000
	return nil
}

func (c *copyConfig) buildStringMap() error {
	if c.remapStringField == "" {
		return nil
	}
	c.stringMap = map[string]int{}
	if c.remapStringValues != "" {
		for _, pair := range strings.Split(c.remapStringValues, ",") {
			kv := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(kv) != 2 {
				continue
			}
			n, err := strconv.Atoi(strings.TrimSpace(kv[1]))
			if err != nil {
				continue
			}
			c.stringMap[strings.TrimSpace(kv[0])] = n
		}
		return nil
	}
	lines, err := c.runMysqlQuery(fmt.Sprintf("SELECT DISTINCT `%s` FROM %s.%s WHERE `%s` IS NOT NULL ORDER BY `%s`",
		c.remapStringField, c.database, c.sourceTable, c.remapStringField, c.remapStringField))
	if err != nil {
		return err
	}
	used := map[int]bool{}
	next := 100000
	for _, line := range lines {
		if line == "" {
			continue
		}
		for used[next] {
			next++
			if next > 999999 {
				next = 100000
			}
		}
		used[next] = true
		c.stringMap[line] = next
		next++
	}
	return nil
}

func (c *copyConfig) buildIntStringMap() error {
	if c.remapIntStringField == "" {
		return nil
	}
	if strings.TrimSpace(c.remapIntStringValues) == "" {
		return fmt.Errorf("--remap-int-string requires --remap-int-string-values")
	}
	c.intStringMap = map[int]string{}
	for _, pair := range strings.Split(c.remapIntStringValues, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid --remap-int-string-values pair %q, expected int:string or string:int", pair)
		}
		left := strings.TrimSpace(kv[0])
		right := strings.TrimSpace(kv[1])
		if left == "" || right == "" {
			return fmt.Errorf("invalid --remap-int-string-values pair %q: empty key or value", pair)
		}
		if n, err := strconv.Atoi(left); err == nil {
			c.intStringMap[n] = right
			continue
		}
		n, err := strconv.Atoi(right)
		if err != nil {
			return fmt.Errorf("invalid --remap-int-string-values pair %q, one side must be an int", pair)
		}
		c.intStringMap[n] = left
	}
	if len(c.intStringMap) == 0 {
		return fmt.Errorf("--remap-int-string-values did not contain any valid mapping")
	}
	return nil
}

func (c *copyConfig) getPartitions() ([]copyPartitionInfo, error) {
	headers, rows, err := c.runMysqlTableQuery("SHOW PARTITIONS FROM " + c.database + "." + c.sourceTable)
	if err != nil {
		return nil, err
	}
	partitionIdx := findExactColumnIndex(headers, "partitionname", "partition_name")
	dataSizeIdx := findColumnIndex(headers, "datasize", "data_size")
	rowCountIdx := findColumnIndex(headers, "rowcount", "row_count")
	if partitionIdx < 0 {
		return nil, fmt.Errorf("SHOW PARTITIONS did not return PartitionName column; headers=%v", headers)
	}
	var result []copyPartitionInfo
	for _, row := range rows {
		if len(row) <= partitionIdx {
			continue
		}
		info := copyPartitionInfo{name: row[partitionIdx]}
		if dataSizeIdx >= 0 && len(row) > dataSizeIdx {
			info.dataSize = row[dataSizeIdx]
		}
		if rowCountIdx >= 0 && len(row) > rowCountIdx {
			info.rowCount, _ = strconv.ParseInt(row[rowCountIdx], 10, 64)
		}
		result = append(result, info)
	}
	fmt.Printf("Found %d partitions. First partition: %s\n", len(result), firstPartitionName(result))
	return result, nil
}

func (c *copyConfig) getTablets() ([]copyTabletInfo, error) {
	partitions, err := c.getPartitions()
	if err != nil {
		return nil, err
	}
	var tablets []copyTabletInfo
	for _, partition := range partitions {
		headers, rows, err := c.runMysqlTableQuery("SHOW TABLETS FROM " + c.database + "." + c.sourceTable + " PARTITION(" + partition.name + ")")
		if err != nil {
			return nil, err
		}
		tabletIdx := findColumnIndex(headers, "tabletid", "tablet_id")
		backendIdx := findColumnIndex(headers, "backendid", "backend_id", "backend")
		if tabletIdx < 0 {
			tabletIdx = 0
		}
		for _, row := range rows {
			if len(row) <= tabletIdx {
				continue
			}
			tabletID, _ := strconv.ParseInt(row[tabletIdx], 10, 64)
			backendID := int64(0)
			if backendIdx >= 0 && len(row) > backendIdx {
				backendID, _ = strconv.ParseInt(row[backendIdx], 10, 64)
			}
			tablets = append(tablets, copyTabletInfo{
				tabletID:      tabletID,
				partitionName: partition.name,
				backendID:     backendID,
			})
		}
	}
	return tablets, nil
}

func findExactColumnIndex(headers []string, candidates ...string) int {
	for idx, header := range headers {
		normalized := normalizeColumnName(header)
		for _, candidate := range candidates {
			if normalized == normalizeColumnName(candidate) {
				return idx
			}
		}
	}
	return -1
}

func findColumnIndex(headers []string, candidates ...string) int {
	if idx := findExactColumnIndex(headers, candidates...); idx >= 0 {
		return idx
	}
	for idx, header := range headers {
		normalized := normalizeColumnName(header)
		for _, candidate := range candidates {
			if strings.Contains(normalized, normalizeColumnName(candidate)) {
				return idx
			}
		}
	}
	return -1
}

func normalizeColumnName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "")
	return value
}

func firstPartitionName(partitions []copyPartitionInfo) string {
	if len(partitions) == 0 {
		return ""
	}
	return partitions[0].name
}

func (c *copyConfig) buildSelectExpr(col string) string {
	if c.remapIntStringField != "" && col == c.remapIntStringField && len(c.intStringMap) > 0 {
		keys := make([]int, 0, len(c.intStringMap))
		for key := range c.intStringMap {
			keys = append(keys, key)
		}
		sort.Ints(keys)
		var sb strings.Builder
		sb.WriteString("CASE")
		for _, key := range keys {
			value := strings.ReplaceAll(c.intStringMap[key], "'", "''")
			sb.WriteString(fmt.Sprintf(" WHEN `%s` = %d THEN '%s'", col, key, value))
		}
		sb.WriteString(fmt.Sprintf(" ELSE CAST(`%s` AS STRING) END", col))
		return sb.String()
	}
	if c.remapStringField != "" && col == c.remapStringField && len(c.stringMap) > 0 {
		keys := make([]string, 0, len(c.stringMap))
		for key := range c.stringMap {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var sb strings.Builder
		sb.WriteString("CASE")
		for _, key := range keys {
			sb.WriteString(fmt.Sprintf(" WHEN `%s` = '%s' THEN %d", col, strings.ReplaceAll(key, "'", "''"), c.stringMap[key]))
		}
		sb.WriteString(" ELSE 0 END")
		return sb.String()
	}
	if c.remapField != "" && col == c.remapField {
		srcRange := c.remapSrcEnd - c.remapSrcStart
		dstRange := c.remapDstEnd - c.remapDstStart
		if strings.HasPrefix(c.remapColType, "DATETIME") || strings.HasPrefix(c.remapColType, "DATEV2") {
			return fmt.Sprintf("FROM_UNIXTIME(%d + (UNIX_TIMESTAMP(`%s`) - %d) * %d / %d)", c.remapDstStart, col, c.remapSrcStart, dstRange, srcRange)
		}
		if c.remapIsMs {
			srcStartMs := c.remapSrcStart * 1000
			srcRangeMs := srcRange * 1000
			dstStartMs := c.remapDstStart * 1000
			dstRangeMs := dstRange * 1000
			return fmt.Sprintf("CAST(%d + (`%s` - %d) * %d / %d AS BIGINT)", dstStartMs, col, srcStartMs, dstRangeMs, srcRangeMs)
		}
		return fmt.Sprintf("CAST(%d + (`%s` - %d) * %d / %d AS BIGINT)", c.remapDstStart, col, c.remapSrcStart, dstRange, srcRange)
	}
	return "`" + col + "`"
}

func (c *copyConfig) buildSql(partitionName string, columns []string) string {
	colList := make([]string, 0, len(columns))
	selectExprs := make([]string, 0, len(columns))
	for _, col := range columns {
		colList = append(colList, "`"+col+"`")
		selectExprs = append(selectExprs, c.buildSelectExpr(col))
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s PARTITION(%s)",
		c.database, c.targetTable, strings.Join(colList, ", "), strings.Join(selectExprs, ", "), c.database, c.sourceTable, partitionName)
}

func (c *copyConfig) buildTabletSql(tabletID int64, columns []string) string {
	colList := make([]string, 0, len(columns))
	selectExprs := make([]string, 0, len(columns))
	for _, col := range columns {
		colList = append(colList, "`"+col+"`")
		selectExprs = append(selectExprs, c.buildSelectExpr(col))
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s TABLET(%d)",
		c.database, c.targetTable, strings.Join(colList, ", "), strings.Join(selectExprs, ", "), c.database, c.sourceTable, tabletID)
}

func (c *copyConfig) loadCompleted() map[string]bool {
	completed := map[string]bool{}
	file, err := os.Open(c.logFile)
	if err != nil {
		return completed
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) >= 2 && parts[1] == "SUCCESS" {
			completed[parts[0]] = true
		}
	}
	return completed
}

func (c *copyConfig) appendLog(name, status string, startMs, endMs int64, errMsg string) {
	line := fmt.Sprintf("%s|%s|%s|%s|%d|%s\n", name, status,
		time.UnixMilli(startMs).Format("2006-01-02 15:04:05"),
		time.UnixMilli(endMs).Format("2006-01-02 15:04:05"),
		endMs-startMs, errMsg)
	f, err := os.OpenFile(c.logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line)
}

func (c *copyConfig) runPartitionMode(columns []string) error {
	partitions, err := c.getPartitions()
	if err != nil {
		return err
	}
	if c.resume {
		completed := c.loadCompleted()
		filtered := partitions[:0]
		for _, p := range partitions {
			if !completed[p.name] {
				filtered = append(filtered, p)
			}
		}
		partitions = filtered
	}
	if c.dryRun {
		for _, p := range partitions {
			fmt.Println(c.buildSql(p.name, columns))
		}
		return nil
	}
	return c.executeJobs(c.partitionJobs(partitions, columns))
}

func (c *copyConfig) runTabletMode(columns []string) error {
	tablets, err := c.getTablets()
	if err != nil {
		return err
	}
	sort.SliceStable(tablets, func(i, j int) bool {
		if tablets[i].backendID == tablets[j].backendID {
			return tablets[i].tabletID < tablets[j].tabletID
		}
		return tablets[i].backendID < tablets[j].backendID
	})
	if c.resume {
		completed := c.loadCompleted()
		filtered := tablets[:0]
		for _, t := range tablets {
			if !completed[strconv.FormatInt(t.tabletID, 10)] {
				filtered = append(filtered, t)
			}
		}
		tablets = filtered
	}
	if c.dryRun {
		for _, t := range tablets {
			fmt.Println(c.buildTabletSql(t.tabletID, columns))
		}
		return nil
	}
	return c.executeJobs(c.tabletJobs(tablets, columns))
}

func (c *copyConfig) partitionJobs(partitions []copyPartitionInfo, columns []string) []copyJob {
	jobs := make([]copyJob, 0, len(partitions))
	for _, p := range partitions {
		jobs = append(jobs, copyJob{name: p.name, sql: c.buildSql(p.name, columns)})
	}
	return jobs
}

func (c *copyConfig) tabletJobs(tablets []copyTabletInfo, columns []string) []copyJob {
	jobs := make([]copyJob, 0, len(tablets))
	for _, t := range tablets {
		name := strconv.FormatInt(t.tabletID, 10)
		jobs = append(jobs, copyJob{name: name, sql: c.buildTabletSql(t.tabletID, columns)})
	}
	return jobs
}

func (c *copyConfig) executeJobs(jobsList []copyJob) error {
	jobs := make(chan copyJob)
	results := make(chan copyResult, c.concurrency)
	var wg sync.WaitGroup
	var done atomic.Int64
	total := len(jobsList)

	for i := 0; i < c.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				start := time.Now()
				if err := c.runMysqlExec(job.sql); err != nil {
					results <- copyResult{partitionName: job.name, success: false, durationMs: time.Since(start).Milliseconds(), errorMsg: err.Error()}
					continue
				}
				results <- copyResult{partitionName: job.name, success: true, durationMs: time.Since(start).Milliseconds()}
			}
		}()
	}

	go func() {
		for _, job := range jobsList {
			jobs <- job
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	success := 0
	failed := 0
	for r := range results {
		done.Add(1)
		if r.success {
			success++
		} else {
			failed++
		}
		fmt.Printf("[%d/%d] %s %s (%dms)%s\n", done.Load(), total, r.partitionName, map[bool]string{true: "SUCCESS", false: "FAILED"}[r.success], r.durationMs, func() string {
			if r.errorMsg == "" {
				return ""
			}
			return ": " + r.errorMsg
		}())
		c.appendLog(r.partitionName, map[bool]string{true: "SUCCESS", false: "FAILED"}[r.success], time.Now().Add(-time.Duration(r.durationMs)*time.Millisecond).UnixMilli(), time.Now().UnixMilli(), r.errorMsg)
	}
	fmt.Printf("Log written to: %s\n", c.logFile)
	return nil
}

func parseCopyTimeRange(value string) (int64, int64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, 0, nil
	}
	parts := strings.Split(value, ",")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid time range: %s", value)
	}
	start, err := parseCopyDatetime(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, err
	}
	end, err := parseCopyDatetime(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

func parseCopyDatetime(value string) (int64, error) {
	layouts := []string{"2006-01-02 15:04:05", "2006-01-02"}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, value, time.Local); err == nil {
			return t.Unix(), nil
		}
	}
	return 0, fmt.Errorf("invalid datetime: %s", value)
}
