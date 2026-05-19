package ddlparser

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"doris-data-generator-go/config"
)

var typePatterns = map[string]*regexp.Regexp{
	"INT":       regexp.MustCompile(`(?i)\bINT\b`),
	"BIGINT":    regexp.MustCompile(`(?i)\bBIGINT\b`),
	"SMALLINT":  regexp.MustCompile(`(?i)\bSMALLINT\b`),
	"TINYINT":   regexp.MustCompile(`(?i)\bTINYINT\b`),
	"DECIMAL":   regexp.MustCompile(`(?i)\bDECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)`),
	"VARCHAR":   regexp.MustCompile(`(?i)\bVARCHAR\s*\(\s*(\d+)\s*\)`),
	"CHAR":      regexp.MustCompile(`(?i)\bCHAR\s*\(\s*(\d+)\s*\)`),
	"DATETIME":  regexp.MustCompile(`(?i)\bDATETIME(?:\s*\(\s*(\d+)\s*\))?`),
	"DATE":      regexp.MustCompile(`(?i)\bDATE\b`),
	"TIMESTAMP": regexp.MustCompile(`(?i)\bTIMESTAMP(?:\s*\(\s*(\d+)\s*\))?`),
	"DOUBLE":    regexp.MustCompile(`(?i)\bDOUBLE\b`),
	"FLOAT":     regexp.MustCompile(`(?i)\bFLOAT\b`),
	"BOOLEAN":   regexp.MustCompile(`(?i)\bBOOLEAN\b`),
}

var createTablePattern = regexp.MustCompile(`(?is)CREATE\s+TABLE\s+[^\(]+\((.+)\)`)
var columnNamePattern = regexp.MustCompile("^`?(\\w+)`?")

func ParseColumnDefinition(col string) (config.Column, error) {
	col = strings.TrimSpace(col)
	match := columnNamePattern.FindStringSubmatch(col)
	if len(match) < 2 {
		return config.Column{}, fmt.Errorf("cannot parse column name from: %s", col)
	}

	column := config.Column{
		Name:       match[1],
		Type:       "VARCHAR",
		TypeParams: map[string]int{},
		Nullable:   !strings.Contains(strings.ToUpper(col), "NOT NULL"),
	}

	colUpper := strings.ToUpper(col)
	for sqlType, pattern := range typePatterns {
		if !pattern.MatchString(colUpper) {
			continue
		}
		column.Type = sqlType
		submatches := pattern.FindStringSubmatch(col)
		switch sqlType {
		case "DECIMAL":
			if len(submatches) >= 3 {
				column.TypeParams["precision"] = parseIntOrZero(submatches[1])
				column.TypeParams["scale"] = parseIntOrZero(submatches[2])
			}
		case "VARCHAR", "CHAR":
			if len(submatches) >= 2 {
				column.TypeParams["length"] = parseIntOrZero(submatches[1])
			}
		case "DATETIME", "TIMESTAMP":
			if len(submatches) >= 2 && submatches[1] != "" {
				column.TypeParams["scale"] = parseIntOrZero(submatches[1])
			}
		}
		return column, nil
	}

	fallbackPattern := regexp.MustCompile(`(?i)(?:VARCHAR|CHAR|DECIMAL|INT|BIGINT|SMALLINT|TINYINT|DATETIME|DATE|TIMESTAMP|DOUBLE|FLOAT|BOOLEAN)`)
	if fallback := fallbackPattern.FindString(strings.ToUpper(col)); fallback != "" {
		column.Type = fallback
	}
	return column, nil
}

func Parse(ddl string) ([]config.Column, error) {
	match := createTablePattern.FindStringSubmatch(ddl)
	if len(match) < 2 {
		return nil, fmt.Errorf("cannot parse DDL: %s", ddl)
	}

	columnsStr := match[1]
	parts := splitTopLevelColumns(columnsStr)
	constraintKeywords := []string{"PRIMARY KEY", "FOREIGN KEY", "INDEX", "UNIQUE", "CHECK", "CONSTRAINT", "KEY"}

	result := make([]config.Column, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		upper := strings.ToUpper(part)
		skip := false
		for _, keyword := range constraintKeywords {
			if strings.HasPrefix(upper, keyword) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		column, err := ParseColumnDefinition(part)
		if err == nil {
			result = append(result, column)
		}
	}
	return result, nil
}

func ParseFile(filePath string) ([]config.Column, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("SQL file not found: %s", filePath)
	}
	if strings.TrimSpace(string(content)) == "" {
		return nil, fmt.Errorf("SQL file is empty: %s", filePath)
	}
	return Parse(string(content))
}

func DemoToMap(demo string) (map[string]any, error) {
	demo = strings.TrimSpace(demo)
	if demo == "" {
		return nil, fmt.Errorf("demo data is empty")
	}

	if strings.HasPrefix(demo, "{") {
		var payload map[string]any
		if err := json.Unmarshal([]byte(demo), &payload); err == nil {
			return payload, nil
		}
	}

	reader := csv.NewReader(strings.NewReader(demo))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("invalid CSV demo data: %w", err)
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("demo data must have header and at least one data row")
	}

	headers := records[0]
	values := records[1]
	if len(headers) != len(values) {
		return nil, fmt.Errorf("header count (%d) != value count (%d)", len(headers), len(values))
	}

	result := make(map[string]any, len(headers))
	for idx := range headers {
		result[strings.TrimSpace(headers[idx])] = strings.TrimSpace(values[idx])
	}
	return result, nil
}

func DemoToMapFile(filePath string) (map[string]any, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("demo file not found: %s", filePath)
	}

	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return nil, fmt.Errorf("demo file is empty: %s", filePath)
	}

	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		var payload any
		if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
			return nil, fmt.Errorf("invalid JSON file: %w", err)
		}
		switch v := payload.(type) {
		case map[string]any:
			return v, nil
		case []any:
			if len(v) == 0 {
				return nil, fmt.Errorf("JSON must be an object or array with at least one element")
			}
			if first, ok := v[0].(map[string]any); ok {
				return first, nil
			}
			return nil, fmt.Errorf("JSON array first element must be an object")
		default:
			return nil, fmt.Errorf("JSON must be an object or array with at least one element")
		}
	}

	return DemoToMap(trimmed)
}

func splitTopLevelColumns(input string) []string {
	result := make([]string, 0)
	current := strings.Builder{}
	depth := 0
	for _, ch := range input {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			if depth > 0 {
				depth--
			}
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				result = append(result, strings.TrimSpace(current.String()))
				current.Reset()
				continue
			}
			current.WriteRune(ch)
		default:
			current.WriteRune(ch)
		}
	}
	if strings.TrimSpace(current.String()) != "" {
		result = append(result, strings.TrimSpace(current.String()))
	}
	return result
}

func parseIntOrZero(value string) int {
	var parsed int
	fmt.Sscanf(value, "%d", &parsed)
	return parsed
}
