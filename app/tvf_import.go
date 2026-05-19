package app

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type tvfImportConfig struct {
	endpoint        string
	tvfEndpoint     string
	accessKeyID     string
	accessKeySecret string
	bucket          string
	prefix          string
	addressingStyle string
	dorisHost       string
	dorisPort       int
	dorisDatabase   string
	dorisTable      string
	dorisUser       string
	dorisPassword   string
	cluster         string
	batchFiles      int
	concurrency     int
	logTypeFilter   string
	remapString     string
	remapValues     map[string]int
	dryRun          bool
	skipSchemaCheck bool
}

type listBucketResult struct {
	XMLName               xml.Name        `xml:"ListBucketResult"`
	Contents              []ossObjectInfo `xml:"Contents"`
	IsTruncated           bool            `xml:"IsTruncated"`
	NextContinuationToken string          `xml:"NextContinuationToken"`
}

type ossObjectInfo struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}

type tvfResult struct {
	batchIndex int
	success    bool
	durationMs int64
	message    string
	sql        string
}

func runTVFImport(options Options) error {
	cfg := tvfImportConfig{
		endpoint:        normalizeTVFEndpoint(options.OSSEndpoint),
		tvfEndpoint:     strings.TrimSpace(options.OSSEndpoint),
		accessKeyID:     options.OSSAK,
		accessKeySecret: options.OSSSK,
		bucket:          options.OSSBucket,
		prefix:          strings.Trim(options.OSSPath, "/"),
		addressingStyle: strings.ToLower(strings.TrimSpace(options.OSSAddressing)),
		dorisHost:       options.DorisHost,
		dorisPort:       options.DorisPort,
		dorisDatabase:   options.DorisDatabase,
		dorisTable:      options.DorisTable,
		dorisUser:       options.DorisUser,
		dorisPassword:   options.DorisPassword,
		cluster:         strings.TrimSpace(options.TVFCluster),
		batchFiles:      maxInt(1, options.TVFBatchFiles),
		concurrency:     maxInt(1, options.Parallel),
		logTypeFilter:   sanitizeFilenameSuffix(options.TVFLogType),
		remapString:     strings.TrimSpace(options.TVFRemapString),
		dryRun:          options.DryRun,
		skipSchemaCheck: options.SkipSchemaCheck,
	}
	var err error
	cfg.remapValues, err = parseTVFRemapValues(options.TVFRemapValues)
	if err != nil {
		return err
	}
	return cfg.run()
}

func (c tvfImportConfig) run() error {
	fmt.Println("============================================")
	fmt.Println("  S3/OSS TVF Importer")
	fmt.Println("============================================")
	fmt.Printf("Source: s3://%s/%s\n", c.bucket, c.prefix)
	fmt.Printf("Target: %s.%s\n", c.dorisDatabase, c.dorisTable)
	fmt.Printf("Doris:  %s@%s:%d\n", c.dorisUser, c.dorisHost, c.dorisPort)
	fmt.Printf("Batch files: %d, concurrency: %d\n", c.batchFiles, c.concurrency)
	if c.logTypeFilter != "" {
		fmt.Printf("Log type filter: %s\n", c.logTypeFilter)
	}
	if c.remapString != "" {
		fmt.Printf("String remap: %s (%d values)\n", c.remapString, len(c.remapValues))
	}
	if c.cluster != "" {
		fmt.Printf("Cluster: @%s\n", c.cluster)
	}
	if c.skipSchemaCheck {
		fmt.Println("Schema check: skipped")
	}

	files, err := c.listParquetFiles()
	if err != nil {
		return err
	}
	if len(files) == 0 {
		fmt.Println("No parquet files found.")
		return nil
	}
	fmt.Printf("Found %d parquet files after filtering.\n", len(files))

	columns, err := c.getDorisColumns()
	if err != nil {
		return err
	}
	fmt.Printf("Doris table has %d columns.\n", len(columns))

	sqls := c.buildSQLs(files, columns)
	fmt.Printf("Built %d TVF batches.\n", len(sqls))
	if c.dryRun {
		for idx, sqlText := range sqls {
			fmt.Printf("\n--- Batch %d ---\n%s\n", idx+1, sqlText)
		}
		return nil
	}

	results := c.executeSQLs(sqls)
	success := 0
	failed := 0
	for _, result := range results {
		if result.success {
			success++
		} else {
			failed++
		}
	}
	fmt.Println("\n========== IMPORT SUMMARY ==========")
	fmt.Printf("Total batches: %d\n", len(results))
	fmt.Printf("Successful:    %d\n", success)
	fmt.Printf("Failed:        %d\n", failed)
	if failed > 0 {
		fmt.Println("\n--- Failed batches ---")
		for _, result := range results {
			if !result.success {
				fmt.Printf("Batch %d: %s\n%s\n", result.batchIndex+1, result.message, result.sql)
			}
		}
	}
	return nil
}

func (c tvfImportConfig) listParquetFiles() ([]string, error) {
	var files []string
	token := ""
	for {
		result, err := c.listObjects(token)
		if err != nil {
			return nil, err
		}
		for _, object := range result.Contents {
			if c.includeParquetObject(object.Key, object.Size) {
				files = append(files, "s3://"+c.bucket+"/"+object.Key)
			}
		}
		if !result.IsTruncated || result.NextContinuationToken == "" {
			break
		}
		token = result.NextContinuationToken
	}
	sort.Strings(files)
	return files, nil
}

func (c tvfImportConfig) includeParquetObject(objectKey string, objectSize int64) bool {
	if objectSize <= 0 {
		return false
	}
	lowerKey := strings.ToLower(objectKey)
	if !strings.HasSuffix(lowerKey, ".parquet") {
		return false
	}
	if c.logTypeFilter == "" {
		return true
	}
	return strings.HasSuffix(lowerKey, "."+strings.ToLower(c.logTypeFilter)+".parquet")
}

func (c tvfImportConfig) listObjects(continuationToken string) (listBucketResult, error) {
	listURL, canonicalResource, err := c.listObjectsURL(continuationToken)
	if err != nil {
		return listBucketResult{}, err
	}
	req, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		return listBucketResult{}, err
	}
	date := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set("Date", date)
	req.Header.Set("Authorization", ossAuthorization(c.accessKeyID, c.accessKeySecret, http.MethodGet, "", "", date, canonicalResource))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return listBucketResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	if err != nil {
		return listBucketResult{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return listBucketResult{}, fmt.Errorf("list objects failed: status=%s body=%s", resp.Status, strings.TrimSpace(string(body)))
	}
	var result listBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return listBucketResult{}, err
	}
	return result, nil
}

func (c tvfImportConfig) listObjectsURL(continuationToken string) (string, string, error) {
	prefix := c.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	query := url.Values{}
	query.Set("list-type", "2")
	query.Set("prefix", prefix)
	if continuationToken != "" {
		query.Set("continuation-token", continuationToken)
	}
	encodedQuery := query.Encode()
	canonicalResource := fmt.Sprintf("/%s/", c.bucket)
	if continuationToken != "" {
		canonicalResource = fmt.Sprintf("/%s/?continuation-token=%s", c.bucket, continuationToken)
	}
	switch c.addressingStyle {
	case "path":
		return fmt.Sprintf("%s/%s?%s", c.endpoint, c.bucket, encodedQuery), canonicalResource, nil
	default:
		endpointURL, err := url.Parse(c.endpoint)
		if err != nil {
			return "", "", err
		}
		endpointURL.Host = c.bucket + "." + endpointURL.Host
		endpointURL.RawQuery = encodedQuery
		return endpointURL.String(), canonicalResource, nil
	}
}

func (c tvfImportConfig) getDorisColumns() ([]string, error) {
	copyCfg := copyConfig{
		host:        c.dorisHost,
		port:        c.dorisPort,
		user:        c.dorisUser,
		password:    c.dorisPassword,
		database:    c.dorisDatabase,
		sourceTable: c.dorisTable,
	}
	return copyCfg.getTableColumns(c.dorisTable)
}

func (c tvfImportConfig) buildSQLs(files []string, columns []string) []string {
	var sqls []string
	for start := 0; start < len(files); start += c.batchFiles {
		end := minInt(len(files), start+c.batchFiles)
		sqls = append(sqls, c.buildSQL(files[start:end], columns))
	}
	return sqls
}

func (c tvfImportConfig) buildSQL(files []string, columns []string) string {
	columnList := quoteColumns(columns)
	selectList := c.selectExpressions(columns)
	uri := tvfURI(files)
	credentials := []string{
		fmt.Sprintf(`"s3.access_key" = "%s"`, escapeTVFString(c.accessKeyID)),
		fmt.Sprintf(`"s3.secret_key" = "%s"`, escapeTVFString(c.accessKeySecret)),
	}
	if c.tvfEndpoint != "" {
		credentials = append(credentials, fmt.Sprintf(`"s3.endpoint" = "%s"`, escapeTVFString(c.tvfEndpoint)))
	}
	insertSQL := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s)\nSELECT %s FROM s3(\n    \"uri\" = \"%s\",\n    \"format\" = \"parquet\",\n    %s\n)",
		c.dorisDatabase,
		c.dorisTable,
		columnList,
		selectList,
		escapeTVFString(uri),
		strings.Join(credentials, ",\n    "),
	)
	if c.cluster == "" {
		return insertSQL
	}
	return fmt.Sprintf("USE @%s;\n%s", escapeClusterName(c.cluster), insertSQL)
}

func (c tvfImportConfig) selectExpressions(columns []string) string {
	expressions := make([]string, 0, len(columns))
	for _, column := range columns {
		if c.remapString != "" && column == c.remapString {
			expressions = append(expressions, buildTVFStringRemapExpr(column, c.remapValues))
			continue
		}
		expressions = append(expressions, quoteColumn(column))
	}
	return strings.Join(expressions, ", ")
}

func buildTVFStringRemapExpr(column string, values map[string]int) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteString("CASE")
	for _, key := range keys {
		builder.WriteString(fmt.Sprintf(" WHEN %s = '%s' THEN %d", quoteColumn(column), strings.ReplaceAll(key, "'", "''"), values[key]))
	}
	builder.WriteString(" ELSE 0 END")
	return builder.String()
}

func (c tvfImportConfig) executeSQLs(sqls []string) []tvfResult {
	jobs := make(chan tvfResult)
	results := make(chan tvfResult, len(sqls))
	var wg sync.WaitGroup
	var done atomic.Int64
	copyCfg := copyConfig{
		host:     c.dorisHost,
		port:     c.dorisPort,
		user:     c.dorisUser,
		password: c.dorisPassword,
		database: c.dorisDatabase,
	}
	for worker := 0; worker < c.concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				start := time.Now()
				err := copyCfg.runMysqlExec(job.sql)
				job.durationMs = time.Since(start).Milliseconds()
				if err != nil {
					job.success = false
					job.message = err.Error()
				} else {
					job.success = true
					job.message = "OK"
				}
				current := done.Add(1)
				fmt.Printf("[%d/%d] batch %d %s (%dms)\n", current, len(sqls), job.batchIndex+1, map[bool]string{true: "SUCCESS", false: "FAILED"}[job.success], job.durationMs)
				results <- job
			}
		}()
	}
	go func() {
		for idx, sqlText := range sqls {
			jobs <- tvfResult{batchIndex: idx, sql: sqlText}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()
	var collected []tvfResult
	for result := range results {
		collected = append(collected, result)
	}
	sort.Slice(collected, func(i, j int) bool { return collected[i].batchIndex < collected[j].batchIndex })
	return collected
}

func tvfURI(files []string) string {
	if len(files) == 1 {
		return files[0]
	}
	prefix := commonURIPathPrefix(files)
	names := make([]string, 0, len(files))
	for _, file := range files {
		names = append(names, strings.TrimPrefix(file, prefix))
	}
	return prefix + "{" + strings.Join(names, ",") + "}"
}

func commonURIPathPrefix(files []string) string {
	first := files[0]
	lastSlash := strings.LastIndex(first, "/")
	if lastSlash < 0 {
		return ""
	}
	return first[:lastSlash+1]
}

func quoteColumns(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, quoteColumn(column))
	}
	return strings.Join(quoted, ", ")
}

func quoteColumn(column string) string {
	return "`" + strings.ReplaceAll(column, "`", "``") + "`"
}

func escapeClusterName(cluster string) string {
	cluster = strings.TrimSpace(cluster)
	var builder strings.Builder
	for _, r := range cluster {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '_' || r == '-' || r == '.':
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func parseTVFRemapValues(raw string) (map[string]int, error) {
	values := map[string]int{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return values, nil
	}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid --tvf-remap-string-values pair %q, expected key:int", pair)
		}
		key := strings.TrimSpace(kv[0])
		if key == "" {
			return nil, fmt.Errorf("invalid --tvf-remap-string-values pair %q: empty key", pair)
		}
		value, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid --tvf-remap-string-values pair %q: %w", pair, err)
		}
		values[key] = value
	}
	return values, nil
}

func normalizeTVFEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return endpoint
	}
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	return strings.TrimRight(endpoint, "/")
}

func ossAuthorization(accessKeyID, accessKeySecret, method, contentMD5, contentType, date, canonicalResource string) string {
	stringToSign := strings.Join([]string{method, contentMD5, contentType, date, canonicalResource}, "\n")
	mac := hmac.New(sha1.New, []byte(accessKeySecret))
	_, _ = mac.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("OSS %s:%s", accessKeyID, signature)
}

func escapeTVFString(value string) string {
	return strings.ReplaceAll(value, `"`, `\"`)
}
