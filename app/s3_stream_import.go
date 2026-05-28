package app

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"doris-data-generator-go/writer"
)

type s3StreamLoadJob struct {
	index  int
	object writer.S3Object
}

type s3StreamLoadResult struct {
	index      int
	objectKey  string
	size       int64
	success    bool
	durationMs int64
	message    string
}

func runS3StreamLoadImport(options Options) error {
	prefix := strings.Trim(options.OSSPath, "/")
	s3Client := writer.NewS3Client(
		options.OSSEndpoint,
		options.OSSAK,
		options.OSSSK,
		options.S3Region,
		options.OSSBucket,
		options.OSSAddressing,
		time.Hour,
	)
	dorisWriter := writer.NewDorisWriter(
		options.DorisHost,
		options.DorisPort,
		options.DorisDatabase,
		options.DorisTable,
		options.DorisUser,
		options.DorisPassword,
		time.Hour,
		options.GroupCommit,
	)

	fmt.Println("============================================")
	fmt.Println("  S3 Stream Load Importer")
	fmt.Println("============================================")
	fmt.Printf("Source: s3://%s/%s\n", options.OSSBucket, prefix)
	fmt.Printf("Endpoint: %s, region: %s, addressing: %s\n", options.OSSEndpoint, options.S3Region, options.OSSAddressing)
	fmt.Printf("Target: %s.%s\n", options.DorisDatabase, options.DorisTable)
	fmt.Printf("Doris:  %s@%s:%d\n", options.DorisUser, options.DorisHost, options.DorisPort)
	fmt.Printf("Concurrency: %d\n", resolveStreamLoadParallelism(options.StreamLoadParallel, maxInt(1, options.Parallel)))
	if options.OrderedStreamLoad {
		fmt.Println("Ordered Stream Load: enabled")
	}
	if options.TVFLogType != "" {
		fmt.Printf("Log type filter: %s\n", options.TVFLogType)
	}
	if options.GroupCommit {
		fmt.Println("Group commit: enabled")
	}

	objects, err := s3Client.ListParquetObjects(prefix)
	if err != nil {
		return err
	}
	objects = filterS3ObjectsByLogType(objects, options.TVFLogType)
	if len(objects) == 0 {
		fmt.Println("No parquet files found.")
		return nil
	}
	fmt.Printf("Found %d parquet files after filtering.\n", len(objects))
	if options.DryRun {
		for idx, object := range objects {
			fmt.Printf("[%d] s3://%s/%s size=%d\n", idx+1, options.OSSBucket, object.Key, object.Size)
		}
		return nil
	}

	results := executeS3StreamLoads(objects, s3Client, dorisWriter, options)
	success := 0
	failed := 0
	for _, result := range results {
		if result.success {
			success++
		} else {
			failed++
		}
	}

	fmt.Println("\n========== STREAM LOAD SUMMARY ==========")
	fmt.Printf("Total files: %d\n", len(results))
	fmt.Printf("Successful:   %d\n", success)
	fmt.Printf("Failed:       %d\n", failed)
	if failed > 0 {
		fmt.Println("\n--- Failed files ---")
		for _, result := range results {
			if !result.success {
				fmt.Printf("%s: %s\n", result.objectKey, result.message)
			}
		}
		return fmt.Errorf("s3 stream load import failed: %d failed, %d succeeded", failed, success)
	}
	return nil
}

func executeS3StreamLoads(objects []writer.S3Object, s3Client *writer.S3Client, dorisWriter *writer.DorisWriter, options Options) []s3StreamLoadResult {
	workers := resolveStreamLoadParallelism(options.StreamLoadParallel, maxInt(1, options.Parallel))
	if options.OrderedStreamLoad {
		workers = 1
	}
	jobs := make(chan s3StreamLoadJob)
	results := make(chan s3StreamLoadResult, len(objects))
	var wg sync.WaitGroup
	var done atomic.Int64

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				result := runOneS3StreamLoad(job, s3Client, dorisWriter, options)
				current := done.Add(1)
				status := "SUCCESS"
				if !result.success {
					status = "FAILED"
				}
				message := ""
				if !result.success && result.message != "" {
					message = " error=" + result.message
				}
				fmt.Printf("[%d/%d] file %d %s size=%d duration=%dms key=%s%s\n",
					current, len(objects), job.index+1, status, result.size, result.durationMs, result.objectKey, message)
				results <- result
			}
		}()
	}

	go func() {
		for idx, object := range objects {
			jobs <- s3StreamLoadJob{index: idx, object: object}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	collected := make([]s3StreamLoadResult, 0, len(objects))
	for result := range results {
		collected = append(collected, result)
	}
	return collected
}

func runOneS3StreamLoad(job s3StreamLoadJob, s3Client *writer.S3Client, dorisWriter *writer.DorisWriter, options Options) s3StreamLoadResult {
	start := time.Now()
	result := s3StreamLoadResult{
		index:     job.index,
		objectKey: job.object.Key,
		size:      job.object.Size,
	}
	body, contentLength, err := s3Client.OpenObjectBody(job.object.Key)
	if err != nil {
		result.durationMs = time.Since(start).Milliseconds()
		result.message = err.Error()
		return result
	}
	defer body.Close()

	streamResult, err := dorisWriter.WriteReopenableReader(body, contentLength, "parquet", map[string]string{
		"Content-Type": "application/octet-stream",
	}, func() (io.ReadCloser, error) {
		reopened, _, err := s3Client.OpenObjectBody(job.object.Key)
		return reopened, err
	})
	result.durationMs = time.Since(start).Milliseconds()
	if err != nil {
		result.message = err.Error()
		return result
	}
	if options.Debug {
		logStreamLoadResult(streamResult)
	}
	if status := stringValue(streamResult.Payload["Status"]); status != "" && status != "Success" {
		result.message = fmt.Sprintf("doris stream load failed: http=%s status=%s payload=%s", streamResult.HTTPStatus, status, streamResult.Body)
		return result
	}
	result.success = true
	result.message = "OK"
	return result
}

func filterS3ObjectsByLogType(objects []writer.S3Object, logType string) []writer.S3Object {
	logType = sanitizeFilenameSuffix(logType)
	if logType == "" {
		return objects
	}
	suffix := "." + strings.ToLower(logType) + ".parquet"
	filtered := make([]writer.S3Object, 0, len(objects))
	for _, object := range objects {
		if strings.HasSuffix(strings.ToLower(object.Key), suffix) {
			filtered = append(filtered, object)
		}
	}
	return filtered
}
