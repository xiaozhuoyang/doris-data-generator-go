package writer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type JSONLWriter struct {
	outputDir string
}

func NewJSONLWriter(outputDir string) (*JSONLWriter, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, err
	}
	return &JSONLWriter{outputDir: outputDir}, nil
}

func (w *JSONLWriter) WritePartitioned(data []map[string]any, baseFilename string, partitionIdx int) (string, error) {
	filename := fmt.Sprintf("%s.%04d.jsonl", baseFilename, partitionIdx)
	return w.writeRows(data, filename)
}

func (w *JSONLWriter) WritePartitionedWithSuffix(data []map[string]any, baseFilename string, partitionIdx int, suffix string) (string, error) {
	filename := fmt.Sprintf("%s.%04d.%s.jsonl", baseFilename, partitionIdx, suffix)
	return w.writeRows(data, filename)
}

func (w *JSONLWriter) writeRows(data []map[string]any, filename string) (string, error) {
	outputPath := filepath.Join(w.outputDir, filename)

	file, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, row := range data {
		if err := encoder.Encode(row); err != nil {
			return "", err
		}
	}
	return outputPath, nil
}
