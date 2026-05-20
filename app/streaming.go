package app

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"doris-data-generator-go/config"
	"doris-data-generator-go/factory"
	"doris-data-generator-go/writer"
)

type batchTask struct {
	PartitionIdx       int
	BatchIdx           int
	BatchSize          int
	TimestampOffset    int64
	PartitionDateRange []string
}

type batchResult struct {
	PartitionIdx int
	BatchIdx     int
	Rows         []map[string]any
	Err          error
}

type streamLoadBatchTask struct {
	BatchIdx           int64
	BatchSize          int
	TimestampOffset    int64
	PartitionDateRange []string
}

type streamLoadBatch struct {
	BatchIdx int64
	Rows     []map[string]any
}

func generatePartitionRowsParallel(
	columns []config.Column,
	genConfig config.GeneratorConfig,
	fieldConfigs map[string]config.FieldConfig,
	partitionIdx int,
	partitionRows int,
	timestampOffset int64,
	partitionDateRange []string,
	parallelism int,
	chunkSize int,
) ([]map[string]any, error) {
	if partitionRows <= 0 {
		return nil, nil
	}
	if parallelism <= 1 {
		localFactory := factory.NewDataFactory(genConfig, cloneFieldConfigs(fieldConfigs))
		localFactory.SetSequenceOffset(timestampOffset)
		localFactory.SetTimestampOffset(timestampOffset)
		if len(partitionDateRange) > 0 {
			localFactory.SetPartitionDateRange(partitionDateRange)
		}
		return localFactory.GenerateBatch(columns, partitionRows), nil
	}
	if chunkSize <= 0 {
		chunkSize = 10000
	}

	tasks := make([]batchTask, 0, (partitionRows+chunkSize-1)/chunkSize)
	rowsGenerated := 0
	batchIdx := 0
	for rowsGenerated < partitionRows {
		batchSize := minInt(chunkSize, partitionRows-rowsGenerated)
		tasks = append(tasks, batchTask{
			PartitionIdx:       partitionIdx,
			BatchIdx:           batchIdx,
			BatchSize:          batchSize,
			TimestampOffset:    timestampOffset + int64(rowsGenerated),
			PartitionDateRange: partitionDateRange,
		})
		rowsGenerated += batchSize
		batchIdx++
	}

	taskCh := make(chan batchTask)
	resultCh := make(chan batchResult, len(tasks))
	var wg sync.WaitGroup

	for workerIdx := 0; workerIdx < parallelism; workerIdx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				localFactory := factory.NewDataFactory(genConfig, cloneFieldConfigs(fieldConfigs))
				localFactory.SetSequenceOffset(task.TimestampOffset)
				localFactory.SetTimestampOffset(task.TimestampOffset)
				if len(task.PartitionDateRange) > 0 {
					localFactory.SetPartitionDateRange(task.PartitionDateRange)
				}
				rows := localFactory.GenerateBatch(columns, task.BatchSize)
				resultCh <- batchResult{
					PartitionIdx: task.PartitionIdx,
					BatchIdx:     task.BatchIdx,
					Rows:         rows,
				}
			}
		}()
	}

	go func() {
		for _, task := range tasks {
			taskCh <- task
		}
		close(taskCh)
		wg.Wait()
		close(resultCh)
	}()

	buffered := make(map[int][]map[string]any, len(tasks))
	for result := range resultCh {
		if result.Err != nil {
			return nil, result.Err
		}
		buffered[result.BatchIdx] = result.Rows
	}

	keys := make([]int, 0, len(buffered))
	for key := range buffered {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	rows := make([]map[string]any, 0, partitionRows)
	for _, key := range keys {
		rows = append(rows, buffered[key]...)
	}
	return rows, nil
}

func runDorisOnlyStreaming(
	options Options,
	columns []config.Column,
	genConfig config.GeneratorConfig,
	fieldConfigs map[string]config.FieldConfig,
	numPartitions int,
	rowsPerFile int,
	partitionDates [][]string,
	dorisWriter *writer.DorisWriter,
	start time.Time,
) error {
	generatorCount := maxInt(1, options.Parallel)
	writerCount := resolveStreamLoadParallelism(options.StreamLoadParallel, generatorCount)
	batchSize := maxInt(1, options.DorisBatchSize)
	bufferSize := maxInt(1, options.PipelineBuffer) * maxInt(generatorCount, writerCount)
	taskCh := make(chan streamLoadBatchTask, bufferSize)
	batchCh := make(chan streamLoadBatch, bufferSize)
	errCh := make(chan error, generatorCount+writerCount+1)
	done := make(chan struct{})
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			close(done)
		})
	}
	var generatedRows atomic.Int64
	var writtenRows atomic.Int64
	var generators sync.WaitGroup
	var writers sync.WaitGroup

	for generatorIdx := 0; generatorIdx < generatorCount; generatorIdx++ {
		generators.Add(1)
		go func() {
			defer generators.Done()
			localFactory := factory.NewDataFactory(genConfig, cloneFieldConfigs(fieldConfigs))
			for task := range taskCh {
				select {
				case <-done:
					return
				default:
				}
				localFactory.ResetCounters()
				localFactory.SetSequenceOffset(task.TimestampOffset)
				localFactory.SetTimestampOffset(task.TimestampOffset)
				if len(task.PartitionDateRange) > 0 {
					localFactory.SetPartitionDateRange(task.PartitionDateRange)
				} else {
					localFactory.SetPartitionDateRange(nil)
				}
				rows := stripInternalFields(localFactory.GenerateBatch(columns, task.BatchSize))
				generatedRows.Add(int64(len(rows)))
				select {
				case batchCh <- streamLoadBatch{BatchIdx: task.BatchIdx, Rows: rows}:
				case <-done:
					return
				}
			}
		}()
	}

	go func() {
		generators.Wait()
		close(batchCh)
	}()

	for writerIdx := 0; writerIdx < writerCount; writerIdx++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for batch := range batchCh {
				result, err := dorisWriter.WriteBatch(batch.Rows)
				if err != nil {
					sendPipelineError(errCh, err)
					cancel()
					return
				}
				if status := stringValue(result["Status"]); status != "" && status != "Success" {
					sendPipelineError(errCh, fmt.Errorf("doris stream load failed: %v", result))
					cancel()
					return
				}
				writtenRows.Add(int64(len(batch.Rows)))
				printStreamLoadProgress(generatedRows.Load(), writtenRows.Load(), int64(options.Rows), start)
			}
		}()
	}

	var taskCount int64
	defer cancel()

	for partitionIdx := 0; partitionIdx < numPartitions; partitionIdx++ {
		rowsStart := partitionIdx * rowsPerFile
		partitionRows := minInt(rowsPerFile, options.Rows-rowsStart)
		if partitionRows <= 0 {
			break
		}

		partitionDateRange := dateRangeAt(partitionDates, partitionIdx)
		for generated := 0; generated < partitionRows; generated += batchSize {
			currentBatchSize := minInt(batchSize, partitionRows-generated)
			task := streamLoadBatchTask{
				BatchIdx:           taskCount,
				BatchSize:          currentBatchSize,
				TimestampOffset:    int64(rowsStart + generated),
				PartitionDateRange: partitionDateRange,
			}
			taskCount++
			if err := pollPipelineError(errCh); err != nil {
				cancel()
				close(taskCh)
				generators.Wait()
				writers.Wait()
				return err
			}
			select {
			case taskCh <- task:
			case err := <-errCh:
				cancel()
				close(taskCh)
				generators.Wait()
				writers.Wait()
				return err
			}
		}
	}

	close(taskCh)
	generators.Wait()
	writers.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func resolveStreamLoadParallelism(configured int, generationParallelism int) int {
	if configured > 0 {
		return configured
	}
	return maxInt(1, generationParallelism)
}

func cloneFieldConfigs(input map[string]config.FieldConfig) map[string]config.FieldConfig {
	result := make(map[string]config.FieldConfig, len(input))
	for key, value := range input {
		cloned := value
		cloned.TypeParams = cloneIntMap(value.TypeParams)
		cloned.Range = cloneFloatSlice(value.Range)
		cloned.DateRange = cloneStringSlice(value.DateRange)
		cloned.Values = cloneAnySlice(value.Values)
		cloned.ValuesWithWeights = cloneWeightedValues(value.ValuesWithWeights)
		cloned.WeightedValues = cloneAnySlice(value.WeightedValues)
		cloned.WeightedWeights = cloneFloatSlice(value.WeightedWeights)
		cloned.LogTypes = cloneWeightedLogTypes(value.LogTypes)
		cloned.LogTypeValues = cloneStringSlice(value.LogTypeValues)
		cloned.LogTypeWeights = cloneFloatSlice(value.LogTypeWeights)
		if value.Length != nil {
			length := *value.Length
			cloned.Length = &length
		}
		result[key] = cloned
	}
	return result
}

func cloneIntMap(input map[string]int) map[string]int {
	if input == nil {
		return nil
	}
	result := make(map[string]int, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneFloatSlice(input []float64) []float64 {
	if input == nil {
		return nil
	}
	return append([]float64(nil), input...)
}

func cloneStringSlice(input []string) []string {
	if input == nil {
		return nil
	}
	return append([]string(nil), input...)
}

func cloneAnySlice(input []any) []any {
	if input == nil {
		return nil
	}
	return append([]any(nil), input...)
}

func cloneWeightedValues(input []config.WeightedValue) []config.WeightedValue {
	if input == nil {
		return nil
	}
	return append([]config.WeightedValue(nil), input...)
}

func cloneWeightedLogTypes(input []config.WeightedLogType) []config.WeightedLogType {
	if input == nil {
		return nil
	}
	return append([]config.WeightedLogType(nil), input...)
}

func dateRangeAt(partitionDates [][]string, idx int) []string {
	if idx >= 0 && idx < len(partitionDates) {
		return cloneStringSlice(partitionDates[idx])
	}
	return nil
}
