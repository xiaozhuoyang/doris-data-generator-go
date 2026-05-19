package writer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
)

type DorisWriter struct {
	host        string
	port        int
	database    string
	table       string
	username    string
	password    string
	timeout     time.Duration
	groupCommit bool
	client      *http.Client
}

func NewDorisWriter(host string, port int, database, table, username, password string, timeout time.Duration, groupCommit bool) *DorisWriter {
	return &DorisWriter{
		host:        host,
		port:        port,
		database:    database,
		table:       table,
		username:    username,
		password:    password,
		timeout:     timeout,
		groupCommit: groupCommit,
		client:      &http.Client{Timeout: timeout},
	}
}

func (w *DorisWriter) WriteBatch(data []map[string]any) (map[string]any, error) {
	if len(data) == 0 {
		return map[string]any{"Status": "Success", "Message": "No data to write"}, nil
	}

	body, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal stream load payload: %w", err)
	}

	url := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", w.host, w.port, w.database, w.table)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build stream load request: %w", err)
	}

	req.SetBasicAuth(w.username, w.password)
	req.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	req.Header.Set("format", "json")
	req.Header.Set("strip_outer_array", "true")
	req.Header.Set("column_separator", ",")
	req.Header.Set("Expect", "100-continue")
	if w.groupCommit {
		req.Header.Set("group_commit", "async_mode")
	} else {
		req.Header.Set("label", generateLabel())
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("stream load failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read stream load response: %w", err)
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse stream load response: %w", err)
	}
	return result, nil
}

func generateLabel() string {
	return fmt.Sprintf("stream_load_%d_%06d", time.Now().UnixNano(), rand.Intn(1000000))
}
