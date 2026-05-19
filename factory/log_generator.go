package factory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

var chineseChars = []rune("的一是不了在人有我他这个们中来上大为和国地到以说时要就出会可也你对生能而子那得于着下自之年过发后作里用道行所然家种事成方多经么去法学如都同现当没动面起看定天分还进好小部其些主样理心她本前开但因只从想实日军者意无力它与长把机十民第公此已工使情明性知全三又关点正业外将两高间由问很最重并物手应战向头文体政美相见被利什二等产或新己制身果加西斯月话合回特代内信表化老给世位次度门任常先海通教儿原东声提威宁马爱接务口增争团论酸根历")

var chineseWords = []string{
	"用户", "登录", "注册", "订单", "支付", "成功", "失败", "错误", "系统", "服务",
	"请求", "响应", "处理", "完成", "开始", "结束", "超时", "重试", "连接", "断开",
	"数据库", "缓存", "队列", "日志", "监控", "告警", "异常", "警告", "信息", "调试",
	"服务器", "客户端", "网络", "协议", "接口", "参数", "结果", "状态", "代码", "配置",
}

var nginxAccessMethods = []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
var nginxAccessPaths = []string{
	"/api/v1/users", "/api/v1/products", "/api/v1/orders", "/api/v1/search", "/api/v1/auth/login",
	"/api/v1/auth/logout", "/api/v1/profile", "/api/v1/settings", "/api/v1/notifications", "/api/v2/feed",
	"/api/v2/recommendations", "/health", "/healthz", "/ready", "/metrics", "/status", "/webhook", "/callback",
}
var nginxStatusCodes = []int{200, 200, 200, 200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503}
var nginxUserAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",
	"curl/7.88.1",
	"python-requests/2.31.0",
	"Go-http-client/2.0",
}

var logLevels = []string{"DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR", "FATAL"}
var appLogMessages = map[string][]string{
	"DEBUG": {"Cache miss for key: user_session_{id}", "SQL query executed in {ms}ms: SELECT * FROM users", "Memory usage: {mb}MB"},
	"INFO":  {"User {id} logged in from {ip}", "Order {id} created successfully", "API request: {method} {path} - {status}"},
	"WARN":  {"Slow query detected: {ms}ms", "Retry attempt {n} for job {id}", "Session expired for user {id}"},
	"ERROR": {"Failed to connect to database: connection timeout", "Payment failed for order {id}: insufficient funds", "API error: {method} {path} returned {status}"},
	"FATAL": {"Database connection pool exhausted", "Out of memory: cannot allocate {mb}MB", "Critical error in worker process"},
}

type LogGenerator struct{}

func (LogGenerator) Generate(rng *rand.Rand, logType string, baseTime time.Time) string {
	if baseTime.IsZero() {
		baseTime = time.Now()
	}

	switch logType {
	case "nginx_error":
		return generateNginxErrorLog(rng, baseTime)
	case "java_error":
		return generateJavaErrorLog(rng, baseTime)
	case "golang_error":
		return generateGolangErrorLog(baseTime)
	case "python_error":
		return generatePythonErrorLog(rng, baseTime)
	case "app_log":
		return generateApplicationLog(rng, baseTime)
	case "json_log":
		return generateJSONLog(rng, baseTime)
	case "json_log_large":
		return generateLargeJSONLog(rng, baseTime)
	case "cloudwatch_json":
		return generateCloudWatchLog(rng, baseTime)
	case "k8s_json":
		return generateK8sLog(rng, baseTime)
	default:
		return generateNginxAccessLog(rng, baseTime)
	}
}

func generateNginxAccessLog(rng *rand.Rand, baseTime time.Time) string {
	ip := fmt.Sprintf("%d.%d.%d.%d", randInt(rng, 1, 255), randInt(rng, 1, 255), randInt(rng, 1, 255), randInt(rng, 1, 255))
	remoteUser := "-"
	if rng.Float64() <= 0.3 {
		remoteUser = fmt.Sprintf("user%d", randInt(rng, 1000, 9999))
	}

	path := randomChoice(rng, nginxAccessPaths)
	if rng.Float64() > 0.8 {
		path = fmt.Sprintf("%s?id=%d&page=%d", path, randInt(rng, 1, 10000), randInt(rng, 1, 100))
	}

	return fmt.Sprintf(`%s - %s [%s] "%s %s %s" %d %d "%s" "%s"`,
		ip,
		remoteUser,
		baseTime.UTC().Format("02/Jan/2006:15:04:05 +0000"),
		randomChoice(rng, nginxAccessMethods),
		path,
		randomChoice(rng, []string{"HTTP/1.1", "HTTP/2.0"}),
		randomChoice(rng, nginxStatusCodes),
		randInt(rng, 100, 500000),
		randomReferer(rng),
		randomChoice(rng, nginxUserAgents),
	)
}

func generateNginxErrorLog(rng *rand.Rand, baseTime time.Time) string {
	errors := []string{
		"connect() failed (111: Connection refused) while connecting to upstream",
		"no live upstream while connecting to upstream",
		"upstream timed out (110: Connection timed out)",
		`open() "/usr/share/nginx/html/maintenance.html" failed`,
	}
	return fmt.Sprintf("%s [%s] %d#%d: *%d %s",
		baseTime.Format("2006/01/02 15:04:05"),
		randomChoice(rng, []string{"error", "warn", "crit"}),
		randInt(rng, 1000, 65535),
		randInt(rng, 1000, 65535),
		randInt(rng, 1000, 65535),
		randomChoice(rng, errors),
	)
}

func generateJavaErrorLog(rng *rand.Rand, baseTime time.Time) string {
	errorType := randomChoice(rng, []string{"NullPointerException", "ArrayIndexOutOfBoundsException", "SQLException", "RuntimeException"})
	errorMsg := randomChoice(rng, []string{"Cannot invoke method on null object", "Array index out of bounds: 5", "Connection refused to database server", "Unexpected error occurred"})
	lines := []string{
		fmt.Sprintf("%s ERROR [main] c.e.d.Application - %s: %s", baseTime.Format("2006-01-02 15:04:05"), errorType, errorMsg),
		"\tat com.example.demo.Service.method(Service.java:42)",
		"\tat com.example.demo.Controller.handle(Controller.java:28)",
		"\tat java.lang.reflect.Method.invoke(Method.java:498)",
	}
	return strings.Join(lines, "\n")
}

func generateGolangErrorLog(baseTime time.Time) string {
	lines := []string{
		fmt.Sprintf("%s ERROR [main] app/main.go:42 - Failed to connect to database", baseTime.UTC().Format(time.RFC3339)),
		"\terror=connection refused",
		"\tstacktrace=goroutine 1 [running]:",
		"\tmain.main()",
		"\t\tapp/main.go:42 +0x...",
	}
	return strings.Join(lines, "\n")
}

func generatePythonErrorLog(rng *rand.Rand, baseTime time.Time) string {
	errorType := randomChoice(rng, []string{"KeyError", "ValueError", "TypeError", "RuntimeError"})
	errorMsg := randomChoice(rng, []string{"'user_id'", "'expected str, got int'", "invalid literal for int()", "maximum recursion exceeded"})
	lines := []string{
		fmt.Sprintf("%s ERROR [root] app.py - %s: %s", baseTime.Format("2006-01-02 15:04:05"), errorType, errorMsg),
		"Traceback (most recent call last):",
		`  File "app.py", line 42, in handle_request`,
		`  File "app.py", line 28, in process_data`,
		fmt.Sprintf("%s: %s", errorType, errorMsg),
	}
	return strings.Join(lines, "\n")
}

func generateApplicationLog(rng *rand.Rand, baseTime time.Time) string {
	level := randomChoice(rng, logLevels)
	msgTemplate := randomChoice(rng, appLogMessages[level])
	msg := strings.NewReplacer(
		"{id}", fmt.Sprintf("%d", randInt(rng, 1000, 999999)),
		"{ip}", fmt.Sprintf("%d.%d.%d.%d", randInt(rng, 1, 255), randInt(rng, 1, 255), randInt(rng, 1, 255), randInt(rng, 1, 255)),
		"{method}", randomChoice(rng, nginxAccessMethods),
		"{path}", randomChoice(rng, nginxAccessPaths),
		"{status}", fmt.Sprintf("%d", randomChoice(rng, nginxStatusCodes)),
		"{ms}", fmt.Sprintf("%d", randInt(rng, 1, 5000)),
		"{mb}", fmt.Sprintf("%d", randInt(rng, 64, 8192)),
		"{n}", fmt.Sprintf("%d", randInt(rng, 1, 3)),
	).Replace(msgTemplate)
	return fmt.Sprintf("%s %s [%s] - %s",
		baseTime.Format("2006-01-02 15:04:05.000"),
		level,
		randomChoice(rng, []string{"main", "worker", "api", "auth", "payment"}),
		msg,
	)
}

func generateJSONLog(rng *rand.Rand, baseTime time.Time) string {
	level := randomChoice(rng, logLevels)
	payload := map[string]any{
		"timestamp": baseTime.UTC().Format("2006-01-02T15:04:05.000000Z"),
		"level":     level,
		"service":   randomChoice(rng, []string{"user-service", "order-service", "payment-service", "api-gateway"}),
		"trace_id":  randomHex(rng, 32),
		"span_id":   randomHex(rng, 16),
		"message":   generateApplicationLog(rng, baseTime),
		"user_id":   pickOptionalUserID(rng),
	}
	return marshalLogJSON(payload)
}

func generateLargeJSONLog(rng *rand.Rand, baseTime time.Time) string {
	payload := map[string]any{
		"timestamp":      baseTime.UTC().Format("2006-01-02T15:04:05.000000Z"),
		"level":          randomChoice(rng, logLevels),
		"service":        randomChoice(rng, []string{"user-service", "order-service", "payment-service", "api-gateway", "go-service"}),
		"trace_id":       randomHex(rng, 32),
		"span_id":        randomHex(rng, 16),
		"parent_span_id": randomHex(rng, 16),
		"env":            randomChoice(rng, []string{"production", "staging", "dev"}),
		"region":         randomChoice(rng, []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}),
		"message":        randomChoice(rng, []string{"User action completed successfully", "Order processed", "Payment completed", "Background job finished"}),
		"user": map[string]any{
			"user_id":    randInt(rng, 1000, 999999),
			"username":   fmt.Sprintf("user%d", randInt(rng, 1000, 9999)),
			"email":      fmt.Sprintf("user%d@example.com", randInt(rng, 100, 999)),
			"ip":         fmt.Sprintf("%d.%d.%d.%d", randInt(rng, 1, 255), randInt(rng, 1, 255), randInt(rng, 1, 255), randInt(rng, 1, 255)),
			"user_agent": randomChoice(rng, nginxUserAgents),
			"session_id": randomHex(rng, 32),
		},
		"request": map[string]any{
			"method":      randomChoice(rng, nginxAccessMethods),
			"path":        randomChoice(rng, nginxAccessPaths),
			"query":       fmt.Sprintf("id=%d&page=%d", randInt(rng, 1, 1000), randInt(rng, 1, 100)),
			"body_size":   randInt(rng, 100, 50000),
			"duration_ms": randInt(rng, 1, 5000),
		},
	}
	return marshalLogJSON(payload)
}

func generateCloudWatchLog(rng *rand.Rand, baseTime time.Time) string {
	payload := map[string]any{
		"logType":   "API_ACCESS",
		"timestamp": baseTime.UnixMilli(),
		"requestId": randomHex(rng, 36),
		"level":     randomChoice(rng, []string{"INFO", "WARN", "ERROR", "DEBUG"}),
		"message":   randomChoice(rng, []string{"Started request", "Completed request", "Request failed", "Health check passed"}),
	}
	return marshalLogJSON(payload)
}

func generateK8sLog(rng *rand.Rand, baseTime time.Time) string {
	payload := map[string]any{
		"timestamp": baseTime.UTC().Format("2006-01-02T15:04:05.000000Z"),
		"level":     randomChoice(rng, []string{"INFO", "WARN", "ERROR", "DEBUG"}),
		"message":   randomChoice(rng, []string{"Request processed successfully", "Health check passed", "Connection established", "Job completed"}),
		"kubernetes": map[string]any{
			"pod_name":       fmt.Sprintf("user-service-%d-%s", randInt(rng, 1000, 9999), randomLower(rng, 5)),
			"namespace":      randomChoice(rng, []string{"production", "staging", "default"}),
			"container_name": randomChoice(rng, []string{"user-service", "api-gateway", "order-processor"}),
			"host":           fmt.Sprintf("node-%d.cluster.local", randInt(rng, 1, 10)),
		},
	}
	return marshalLogJSON(payload)
}

func marshalLogJSON(payload any) string {
	buffer := bytes.Buffer{}
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		return "{}"
	}
	return strings.TrimSpace(buffer.String())
}

func IsJSONLogType(logType string) bool {
	switch strings.ToLower(strings.TrimSpace(logType)) {
	case "json_log", "json_log_large", "cloudwatch_json", "k8s_json":
		return true
	default:
		return false
	}
}

func InjectChineseIntoJSONLog(rng *rand.Rand, text string) string {
	var payload any
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return text
	}
	injectChineseIntoJSONValue(rng, payload)
	return marshalLogJSON(payload)
}

func injectChineseIntoJSONValue(rng *rand.Rand, value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if text, ok := item.(string); ok {
				typed[key] = injectChineseIntoJSONString(rng, text)
				continue
			}
			injectChineseIntoJSONValue(rng, item)
		}
	case []any:
		for _, item := range typed {
			injectChineseIntoJSONValue(rng, item)
		}
	}
}

func injectChineseIntoJSONString(rng *rand.Rand, text string) string {
	if text == "" || rng.Float64() > 0.35 {
		return text
	}
	replacements := [][2]string{
		{"GET", "获取"}, {"POST", "提交"}, {"PUT", "更新"}, {"DELETE", "删除"},
		{"users", "用户"}, {"orders", "订单"}, {"login", "登录"}, {"logout", "登出"},
		{"health", "健康检查"}, {"api", "接口"}, {"search", "搜索"}, {"request", "请求"},
		{"completed", "完成"}, {"failed", "失败"}, {"Payment", "支付"}, {"Order", "订单"},
	}
	result := text
	for _, pair := range replacements {
		if rng.Float64() < 0.5 {
			result = strings.ReplaceAll(result, pair[0], pair[1])
		}
	}
	if result == text && rng.Float64() < 0.5 {
		result = randomChoice(rng, chineseWords) + " " + result
	}
	return result
}

func InjectChineseFull(rng *rand.Rand, text string) string {
	replacements := [][2]string{
		{"GET", "获取"}, {"POST", "提交"}, {"PUT", "更新"}, {"DELETE", "删除"},
		{"200", "成功"}, {"400", "请求错误"}, {"404", "未找到"}, {"500", "服务器错误"},
		{"users", "用户"}, {"orders", "订单"}, {"login", "登录"}, {"logout", "登出"},
		{"health", "健康检查"}, {"api", "接口"},
	}

	result := text
	for _, pair := range replacements {
		if rng.Float64() < 0.5 {
			result = strings.ReplaceAll(result, pair[0], pair[1])
		}
	}

	if rng.Float64() < 0.5 {
		result = randomChoice(rng, []string{"请求 ", "处理 ", "完成 ", "开始 ", "系统 "}) + result
	}
	return result
}

func randInt(rng *rand.Rand, min, max int) int {
	if max <= min {
		return min
	}
	return rng.Intn(max-min+1) + min
}

func randomChoice[T any](rng *rand.Rand, values []T) T {
	return values[rng.Intn(len(values))]
}

func randomHex(rng *rand.Rand, length int) string {
	const alphabet = "0123456789abcdef"
	builder := strings.Builder{}
	builder.Grow(length)
	for idx := 0; idx < length; idx++ {
		builder.WriteByte(alphabet[rng.Intn(len(alphabet))])
	}
	return builder.String()
}

func randomLower(rng *rand.Rand, length int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	builder := strings.Builder{}
	builder.Grow(length)
	for idx := 0; idx < length; idx++ {
		builder.WriteByte(alphabet[rng.Intn(len(alphabet))])
	}
	return builder.String()
}

func randomReferer(rng *rand.Rand) string {
	if rng.Float64() > 0.7 {
		return "-"
	}
	return "https://example.com" + randomChoice(rng, nginxAccessPaths)
}

func pickOptionalUserID(rng *rand.Rand) any {
	if rng.Float64() > 0.3 {
		return randInt(rng, 1, 10000)
	}
	return nil
}
