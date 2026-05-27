package writer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
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

type StreamLoadResult struct {
	HTTPStatusCode int
	HTTPStatus     string
	Body           string
	Payload        map[string]any
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

func (w *DorisWriter) WriteBatch(data []map[string]any) (StreamLoadResult, error) {
	if len(data) == 0 {
		return StreamLoadResult{
			HTTPStatusCode: http.StatusOK,
			HTTPStatus:     http.StatusText(http.StatusOK),
			Body:           `{"Status":"Success","Message":"No data to write"}`,
			Payload: map[string]any{
				"Status":  "Success",
				"Message": "No data to write",
			},
		}, nil
	}

	body, err := json.Marshal(data)
	if err != nil {
		return StreamLoadResult{}, fmt.Errorf("marshal stream load payload: %w", err)
	}
	return w.WriteReader(bytes.NewReader(body), int64(len(body)), "json", map[string]string{
		"Content-Type":      "text/plain; charset=UTF-8",
		"strip_outer_array": "true",
		"column_separator":  ",",
	})
}

func (w *DorisWriter) WriteReader(body io.Reader, contentLength int64, format string, headers map[string]string) (StreamLoadResult, error) {
	url := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", w.host, w.port, w.database, w.table)
	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		return StreamLoadResult{}, fmt.Errorf("build stream load request: %w", err)
	}
	if contentLength >= 0 {
		req.ContentLength = contentLength
	}

	req.SetBasicAuth(w.username, w.password)
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("format", format)
	req.Header.Set("label", generateLabel())
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	if w.groupCommit {
		req.Header.Set("group_commit", "async_mode")
		req.Header.Del("label")
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return StreamLoadResult{}, fmt.Errorf("stream load failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return StreamLoadResult{}, fmt.Errorf("read stream load response: %w", err)
	}

	result := StreamLoadResult{
		HTTPStatusCode: resp.StatusCode,
		HTTPStatus:     resp.Status,
		Body:           strings.TrimSpace(string(respBody)),
	}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result.Payload); err != nil {
			return result, fmt.Errorf("parse stream load response: %w body=%s", err, result.Body)
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, fmt.Errorf("stream load failed: status=%s body=%s", resp.Status, result.Body)
	}
	return result, nil
}

func generateLabel() string {
	return fmt.Sprintf("stream_load_%d_%06d", time.Now().UnixNano(), rand.Intn(1000000))
}
