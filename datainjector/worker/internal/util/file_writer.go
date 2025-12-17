package util

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileWriter 多格式文件写入器接口
type FileWriter interface {
	// Write 写入一条记录
	Write(record map[string]any) error
	// WriteAll 批量写入记录
	WriteAll(records []map[string]any) error
	// Close 关闭文件
	Close() error
	// RecordCount 返回已写入的记录数
	RecordCount() int64
}

// FileWriterConfig 文件写入器配置
type FileWriterConfig struct {
	FilePath string // 文件完整路径
	Format   string // json/csv/parquet
}

// NewFileWriter 创建文件写入器
func NewFileWriter(config FileWriterConfig) (FileWriter, error) {
	// 确保目录存在
	dir := filepath.Dir(config.FilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	switch strings.ToLower(config.Format) {
	case "json", "jsonl":
		return newJSONLinesWriter(config.FilePath)
	case "csv":
		return newCSVWriter(config.FilePath)
	case "parquet":
		// TODO: 实现 Parquet 写入器
		return nil, fmt.Errorf("parquet format not yet implemented")
	default:
		return nil, fmt.Errorf("unsupported format: %s", config.Format)
	}
}

// JSONLinesWriter JSON Lines 格式写入器（每行一个 JSON 对象）
type JSONLinesWriter struct {
	file    *os.File
	encoder *json.Encoder
	count   int64
}

func newJSONLinesWriter(filePath string) (*JSONLinesWriter, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	return &JSONLinesWriter{
		file:    file,
		encoder: json.NewEncoder(file),
	}, nil
}

func (w *JSONLinesWriter) Write(record map[string]any) error {
	if err := w.encoder.Encode(record); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	w.count++
	return nil
}

func (w *JSONLinesWriter) WriteAll(records []map[string]any) error {
	for _, record := range records {
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return nil
}

func (w *JSONLinesWriter) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func (w *JSONLinesWriter) RecordCount() int64 {
	return w.count
}

// CSVWriter CSV 格式写入器
type CSVWriter struct {
	file    *os.File
	writer  *csv.Writer
	headers []string
	count   int64
}

func newCSVWriter(filePath string) (*CSVWriter, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	return &CSVWriter{
		file:   file,
		writer: csv.NewWriter(file),
	}, nil
}

func (w *CSVWriter) Write(record map[string]any) error {
	// 第一次写入时确定表头
	if w.headers == nil {
		w.headers = extractHeaders(record)
		if err := w.writer.Write(w.headers); err != nil {
			return fmt.Errorf("write headers: %w", err)
		}
	}

	// 按表头顺序提取值
	row := make([]string, len(w.headers))
	for i, header := range w.headers {
		if val, ok := record[header]; ok {
			row[i] = ToString(val)
		}
	}

	if err := w.writer.Write(row); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	w.count++
	return nil
}

func (w *CSVWriter) WriteAll(records []map[string]any) error {
	for _, record := range records {
		if err := w.Write(record); err != nil {
			return err
		}
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *CSVWriter) Close() error {
	if w.writer != nil {
		w.writer.Flush()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func (w *CSVWriter) RecordCount() int64 {
	return w.count
}

// extractHeaders 从记录中提取字段名作为 CSV 表头
func extractHeaders(record map[string]any) []string {
	headers := make([]string, 0, len(record))
	for key := range record {
		headers = append(headers, key)
	}
	return headers
}
