package writer

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type OSSWriter struct {
	bucket          string
	prefix          string
	endpoint        string
	accessKeyID     string
	accessKeySecret string
	addressingStyle string
	client          *http.Client
}

func NewOSSWriter(bucket, prefix, endpoint, accessKeyID, accessKeySecret, addressingStyle string, timeout time.Duration) *OSSWriter {
	return &OSSWriter{
		bucket:          bucket,
		prefix:          strings.TrimSpace(prefix),
		endpoint:        normalizeOSSEndpoint(endpoint),
		accessKeyID:     accessKeyID,
		accessKeySecret: accessKeySecret,
		addressingStyle: strings.ToLower(strings.TrimSpace(addressingStyle)),
		client:          &http.Client{Timeout: timeout},
	}
}

func (w *OSSWriter) UploadFile(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return "", err
	}

	objectKey := w.objectKey(filepath.Base(filePath))
	uploadURL, canonicalResource, err := w.uploadURL(objectKey)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest(http.MethodPut, uploadURL, file)
	if err != nil {
		return "", err
	}
	date := time.Now().UTC().Format(http.TimeFormat)
	req.ContentLength = info.Size()
	req.Header.Set("Date", date)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", w.authorization(http.MethodPut, "", "application/octet-stream", date, canonicalResource))

	resp, err := w.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("oss upload failed: status=%s body=%s", resp.Status, strings.TrimSpace(string(body)))
	}
	return objectKey, nil
}

func (w *OSSWriter) objectKey(filename string) string {
	prefix := strings.Trim(w.prefix, "/")
	if prefix == "" {
		return filename
	}
	return path.Join(prefix, filename)
}

func (w *OSSWriter) uploadURL(objectKey string) (string, string, error) {
	escapedObjectKey := escapeOSSObjectKey(objectKey)
	switch w.addressingStyle {
	case "path":
		return fmt.Sprintf("%s/%s/%s", w.endpoint, w.bucket, escapedObjectKey), fmt.Sprintf("/%s/%s", w.bucket, objectKey), nil
	default:
		endpointURL, err := url.Parse(w.endpoint)
		if err != nil {
			return "", "", err
		}
		endpointURL.Host = w.bucket + "." + endpointURL.Host
		endpointURL.Path = "/" + escapedObjectKey
		return endpointURL.String(), fmt.Sprintf("/%s/%s", w.bucket, objectKey), nil
	}
}

func (w *OSSWriter) authorization(method, contentMD5, contentType, date, canonicalResource string) string {
	stringToSign := strings.Join([]string{
		method,
		contentMD5,
		contentType,
		date,
		canonicalResource,
	}, "\n")
	mac := hmac.New(sha1.New, []byte(w.accessKeySecret))
	_, _ = mac.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("OSS %s:%s", w.accessKeyID, signature)
}

func normalizeOSSEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return endpoint
	}
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	return strings.TrimRight(endpoint, "/")
}

func escapeOSSObjectKey(objectKey string) string {
	parts := strings.Split(objectKey, "/")
	for idx, part := range parts {
		parts[idx] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
