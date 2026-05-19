package writer

import "testing"

func TestOSSWriterObjectKey(t *testing.T) {
	writer := NewOSSWriter("bucket", "/logs/generated/", "oss-cn-hangzhou.aliyuncs.com", "ak", "sk", "auto", 0)
	if got := writer.objectKey("data.parquet"); got != "logs/generated/data.parquet" {
		t.Fatalf("unexpected object key: %s", got)
	}
}

func TestOSSWriterUploadURL(t *testing.T) {
	writer := NewOSSWriter("my-bucket", "/logs/", "oss-cn-hangzhou.aliyuncs.com", "ak", "sk", "auto", 0)
	uploadURL, canonicalResource, err := writer.uploadURL("logs/data.parquet")
	if err != nil {
		t.Fatalf("uploadURL returned error: %v", err)
	}
	if uploadURL != "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/logs/data.parquet" {
		t.Fatalf("unexpected upload URL: %s", uploadURL)
	}
	if canonicalResource != "/my-bucket/logs/data.parquet" {
		t.Fatalf("unexpected canonical resource: %s", canonicalResource)
	}
}

func TestOSSWriterPathStyleUploadURL(t *testing.T) {
	writer := NewOSSWriter("my-bucket", "/logs/", "http://127.0.0.1:9000", "ak", "sk", "path", 0)
	uploadURL, canonicalResource, err := writer.uploadURL("logs/data.parquet")
	if err != nil {
		t.Fatalf("uploadURL returned error: %v", err)
	}
	if uploadURL != "http://127.0.0.1:9000/my-bucket/logs/data.parquet" {
		t.Fatalf("unexpected upload URL: %s", uploadURL)
	}
	if canonicalResource != "/my-bucket/logs/data.parquet" {
		t.Fatalf("unexpected canonical resource: %s", canonicalResource)
	}
}
