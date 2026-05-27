package writer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type S3Client struct {
	endpoint        string
	accessKeyID     string
	accessKeySecret string
	region          string
	bucket          string
	addressingStyle string
	client          *http.Client
}

type S3Object struct {
	Key  string
	Size int64
}

func NewS3Client(endpoint, accessKeyID, accessKeySecret, region, bucket, addressingStyle string, timeout time.Duration) *S3Client {
	return &S3Client{
		endpoint:        normalizeS3Endpoint(endpoint),
		accessKeyID:     accessKeyID,
		accessKeySecret: accessKeySecret,
		region:          strings.TrimSpace(region),
		bucket:          bucket,
		addressingStyle: strings.ToLower(strings.TrimSpace(addressingStyle)),
		client:          &http.Client{Timeout: timeout},
	}
}

func (c *S3Client) ListParquetObjects(prefix string) ([]S3Object, error) {
	var objects []S3Object
	token := ""
	for {
		body, err := c.listObjects(prefix, token)
		if err != nil {
			return nil, err
		}
		result, err := parseListBucketResult(body)
		if err != nil {
			return nil, err
		}
		for _, obj := range result.Contents {
			key := strings.TrimSpace(obj.Key)
			if key == "" || !strings.HasSuffix(strings.ToLower(key), ".parquet") {
				continue
			}
			size, _ := strconv.ParseInt(obj.Size, 10, 64)
			if size <= 0 {
				continue
			}
			objects = append(objects, S3Object{Key: key, Size: size})
		}
		if !result.IsTruncated || result.NextContinuationToken == "" {
			break
		}
		token = result.NextContinuationToken
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

func (c *S3Client) OpenObject(objectKey string) ([]byte, error) {
	reader, _, err := c.OpenObjectReader(objectKey)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func (c *S3Client) OpenObjectReader(objectKey string) (io.ReadCloser, int64, error) {
	urlStr, err := c.objectURL(objectKey)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, 0, err
	}
	c.sign(req, "")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, 0, fmt.Errorf("get object failed: status=%s body=%s", resp.Status, strings.TrimSpace(string(body)))
	}
	return resp.Body, resp.ContentLength, nil
}

func (c *S3Client) OpenObjectBody(objectKey string) (io.ReadCloser, int64, error) {
	return c.OpenObjectReader(objectKey)
}

func (c *S3Client) listObjects(prefix, continuationToken string) ([]byte, error) {
	urlStr, err := c.listURL(prefix, continuationToken)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	c.sign(req, "")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("list objects failed: status=%s body=%s", resp.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (c *S3Client) listURL(prefix, continuationToken string) (string, error) {
	query := url.Values{}
	query.Set("list-type", "2")
	query.Set("prefix", prefix)
	if continuationToken != "" {
		query.Set("continuation-token", continuationToken)
	}
	switch c.addressingStyle {
	case "path":
		return fmt.Sprintf("%s/%s?%s", c.endpoint, c.bucket, query.Encode()), nil
	default:
		endpointURL, err := url.Parse(c.endpoint)
		if err != nil {
			return "", err
		}
		endpointURL.Host = c.bucket + "." + endpointURL.Host
		endpointURL.RawQuery = query.Encode()
		return endpointURL.String(), nil
	}
}

func (c *S3Client) objectURL(objectKey string) (string, error) {
	escaped := escapeOSSObjectKey(objectKey)
	switch c.addressingStyle {
	case "path":
		return fmt.Sprintf("%s/%s/%s", c.endpoint, c.bucket, escaped), nil
	default:
		endpointURL, err := url.Parse(c.endpoint)
		if err != nil {
			return "", err
		}
		endpointURL.Host = c.bucket + "." + endpointURL.Host
		endpointURL.Path = "/" + escaped
		return endpointURL.String(), nil
	}
}

func (c *S3Client) sign(req *http.Request, payloadHash string) {
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	shortDate := now.Format("20060102")
	host := req.URL.Host
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("host", host)

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuery := canonicalQueryString(req.URL.Query())
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n", host, payloadHash, amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", shortDate, c.region)
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")
	signingKey := deriveSigningKey(c.accessKeySecret, shortDate, c.region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	authorization := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKeyID, scope, signedHeaders, signature,
	)
	req.Header.Set("Authorization", authorization)
}

type listBucketXML struct {
	Contents []struct {
		Key  string `xml:"Key"`
		Size string `xml:"Size"`
	} `xml:"Contents"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}

func parseListBucketResult(body []byte) (listBucketXML, error) {
	var result listBucketXML
	if err := xml.Unmarshal(body, &result); err != nil {
		return listBucketXML{}, err
	}
	return result, nil
}

func canonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		vals := append([]string(nil), values[key]...)
		sort.Strings(vals)
		for _, value := range vals {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}

func normalizeS3Endpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return endpoint
	}
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	return strings.TrimRight(endpoint, "/")
}
