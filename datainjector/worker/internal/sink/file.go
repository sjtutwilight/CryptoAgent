package sink

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
	"gopkg.in/yaml.v3"
)

type FileConfig struct {
	OutputDir         string
	OutputFormat      string
	FilenamePrefix    string
	MaxRecordsPerFile int64
	RawPayload        bool

	// 元数据配置
	Metadata *MetadataConfig
}

// MetadataConfig 元数据追加配置
type MetadataConfig struct {
	Enabled     bool           `yaml:"enabled"`
	DatasetID   string         `yaml:"dataset_id"`
	Datasource  string         `yaml:"datasource"`
	Domain      string         `yaml:"domain"`
	Category    string         `yaml:"category"`
	Description string         `yaml:"description"`
	Granularity map[string]any `yaml:"granularity"`
	Coverage    map[string]any `yaml:"coverage"`
	Schema      map[string]any `yaml:"schema"`
	Tags        []string       `yaml:"tags"`
	CustomMeta  map[string]any `yaml:"custom_meta"`
}

type FileSink struct {
	mu                sync.Mutex
	outputDir         string
	outputFormat      string
	filenamePrefix    string
	maxRecordsPerFile int64
	rawPayload        bool
	metadataConfig    *MetadataConfig

	writer           util.FileWriter
	rawFile          *os.File
	currentFileIndex int
	recordsInFile    int64
	currentFilePath  string
	fileStartTime    time.Time

	// 元数据追踪
	filesMetadata []FileMetadata
}

func init() {
	Register("file", func(cfg map[string]any) (Sink, error) {
		parsed, err := parseFileConfig(cfg)
		if err != nil {
			return nil, err
		}
		return newFileSink(parsed)
	})
}

func parseFileConfig(cfg map[string]any) (*FileConfig, error) {
	out := &FileConfig{
		OutputFormat:      "json",
		FilenamePrefix:    "data",
		MaxRecordsPerFile: 10000,
		RawPayload:        false,
	}

	out.OutputDir = util.GetString(cfg, "output_dir", "")
	if out.OutputDir == "" {
		return nil, fmt.Errorf("file sink: output_dir required")
	}

	out.OutputFormat = strings.ToLower(util.GetString(cfg, "output_format", out.OutputFormat))
	if out.OutputFormat == "raw" {
		out.RawPayload = true
		out.OutputFormat = "jsonl"
	}
	out.RawPayload = util.GetBool(cfg, "raw_payload", out.RawPayload)

	if prefix := util.GetString(cfg, "filename_prefix", ""); prefix != "" {
		out.FilenamePrefix = prefix
	}
	if maxRecords := util.GetInt(cfg, "max_records_per_file", 0); maxRecords > 0 {
		out.MaxRecordsPerFile = int64(maxRecords)
	}

	// 解析元数据配置
	if metaCfg, ok := cfg["metadata"].(map[string]any); ok {
		if util.GetBool(metaCfg, "enabled", false) {
			out.Metadata = &MetadataConfig{
				Enabled:     true,
				DatasetID:   util.GetString(metaCfg, "dataset_id", ""),
				Datasource:  util.GetString(metaCfg, "datasource", "unknown"),
				Domain:      util.GetString(metaCfg, "domain", ""),
				Category:    util.GetString(metaCfg, "category", ""),
				Description: util.GetString(metaCfg, "description", ""),
			}

			if gran, ok := metaCfg["granularity"].(map[string]any); ok {
				out.Metadata.Granularity = gran
			}
			if cov, ok := metaCfg["coverage"].(map[string]any); ok {
				out.Metadata.Coverage = cov
			}
			if schema, ok := metaCfg["schema"].(map[string]any); ok {
				out.Metadata.Schema = schema
			}
			if tags, ok := metaCfg["tags"].([]any); ok {
				out.Metadata.Tags = make([]string, 0, len(tags))
				for _, t := range tags {
					if s, ok := t.(string); ok {
						out.Metadata.Tags = append(out.Metadata.Tags, s)
					}
				}
			}
			if custom, ok := metaCfg["custom_meta"].(map[string]any); ok {
				out.Metadata.CustomMeta = custom
			}
		}
	}

	return out, nil
}

func newFileSink(cfg *FileConfig) (*FileSink, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("file sink: create output_dir: %w", err)
	}
	return &FileSink{
		outputDir:         cfg.OutputDir,
		outputFormat:      cfg.OutputFormat,
		filenamePrefix:    cfg.FilenamePrefix,
		maxRecordsPerFile: cfg.MaxRecordsPerFile,
		rawPayload:        cfg.RawPayload,
		metadataConfig:    cfg.Metadata,
		filesMetadata:     make([]FileMetadata, 0),
	}, nil
}

func (s *FileSink) Write(msg *types.Message) error {
	if msg == nil || len(msg.Payload) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil && s.rawFile == nil {
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}

	if s.rawPayload {
		if err := s.writeRawLocked(msg.Payload); err != nil {
			return err
		}
	} else {
		var record map[string]any
		if err := json.Unmarshal(msg.Payload, &record); err != nil {
			return fmt.Errorf("file sink: decode payload: %w", err)
		}
		if err := s.writer.Write(record); err != nil {
			return fmt.Errorf("file sink: write record: %w", err)
		}
	}

	s.recordsInFile++
	if s.recordsInFile >= s.maxRecordsPerFile {
		return s.rotateLocked()
	}
	return nil
}

func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 先关闭当前文件并记录元数据
	if err := s.finalizeCurrentFileLocked(); err != nil {
		return err
	}

	if err := s.closeLocked(); err != nil {
		return err
	}

	// 写入元数据片段
	if s.metadataConfig != nil && s.metadataConfig.Enabled && len(s.filesMetadata) > 0 {
		if err := s.writeMetadataFragment(); err != nil {
			// 元数据写入失败不阻塞主流程，仅记录错误
			fmt.Fprintf(os.Stderr, "file sink: failed to write metadata fragment: %v\n", err)
		}
	}

	return nil
}

func (s *FileSink) rotateLocked() error {
	// 先保存当前文件的元数据
	if err := s.finalizeCurrentFileLocked(); err != nil {
		return err
	}

	if err := s.closeLocked(); err != nil {
		return err
	}

	filename := fmt.Sprintf("%s_%03d.%s", s.filenamePrefix, s.currentFileIndex, s.outputFormat)
	filePath := filepath.Join(s.outputDir, filename)

	if s.rawPayload {
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("file sink: open file: %w", err)
		}
		s.rawFile = file
	} else {
		writer, err := util.NewFileWriter(util.FileWriterConfig{
			FilePath: filePath,
			Format:   s.outputFormat,
		})
		if err != nil {
			return fmt.Errorf("file sink: create writer: %w", err)
		}
		s.writer = writer
	}

	s.currentFilePath = filePath
	s.fileStartTime = time.Now()
	s.recordsInFile = 0
	s.currentFileIndex++
	return nil
}

func (s *FileSink) closeLocked() error {
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			return err
		}
		s.writer = nil
	}
	if s.rawFile != nil {
		if err := s.rawFile.Close(); err != nil {
			return err
		}
		s.rawFile = nil
	}
	return nil
}

func (s *FileSink) writeRawLocked(payload []byte) error {
	if s.rawFile == nil {
		return fmt.Errorf("file sink: raw file not initialized")
	}
	if _, err := s.rawFile.Write(payload); err != nil {
		return err
	}
	if len(payload) == 0 || payload[len(payload)-1] != '\n' {
		if _, err := s.rawFile.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

// FileMetadata 文件元数据
type FileMetadata struct {
	Path        string `yaml:"path"`
	SizeBytes   int64  `yaml:"size_bytes"`
	RecordCount int64  `yaml:"record_count"`
	Checksum    string `yaml:"checksum,omitempty"`
	CreatedAt   string `yaml:"created_at"`
}

// DatasetFragment 数据集元数据片段
type DatasetFragment struct {
	ID          string         `yaml:"id"`
	Datasource  string         `yaml:"datasource"`
	Domain      string         `yaml:"domain,omitempty"`
	Category    string         `yaml:"category,omitempty"`
	Description string         `yaml:"description,omitempty"`
	Granularity map[string]any `yaml:"granularity,omitempty"`
	Coverage    map[string]any `yaml:"coverage,omitempty"`
	Schema      map[string]any `yaml:"schema,omitempty"`
	Storage     map[string]any `yaml:"storage,omitempty"`
	Files       []FileMetadata `yaml:"files"`
	Metadata    map[string]any `yaml:"metadata,omitempty"`
}

// finalizeCurrentFileLocked 完成当前文件并记录元数据
func (s *FileSink) finalizeCurrentFileLocked() error {
	if s.currentFilePath == "" || s.recordsInFile == 0 {
		return nil
	}

	if s.metadataConfig == nil || !s.metadataConfig.Enabled {
		return nil
	}

	// 获取文件信息
	fileInfo, err := os.Stat(s.currentFilePath)
	if err != nil {
		return fmt.Errorf("file sink: stat file: %w", err)
	}

	// 计算文件校验和（可选）
	checksum := ""
	if s.metadataConfig.CustomMeta != nil && util.GetBool(s.metadataConfig.CustomMeta, "calculate_checksum", false) {
		checksum, _ = s.calculateChecksum(s.currentFilePath)
	}

	// 计算相对于 runtime/data 的路径
	relPath := s.currentFilePath
	if strings.Contains(relPath, "runtime/data/") {
		parts := strings.SplitN(relPath, "runtime/data/", 2)
		if len(parts) == 2 {
			relPath = parts[1]
		}
	}

	fileMeta := FileMetadata{
		Path:        relPath,
		SizeBytes:   fileInfo.Size(),
		RecordCount: s.recordsInFile,
		Checksum:    checksum,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	s.filesMetadata = append(s.filesMetadata, fileMeta)
	return nil
}

// calculateChecksum 计算文件 MD5 校验和
func (s *FileSink) calculateChecksum(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	buf := make([]byte, 32*1024)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			hash.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}

	return "md5:" + hex.EncodeToString(hash.Sum(nil)), nil
}

// writeMetadataFragment 写入元数据片段到 pending 目录
func (s *FileSink) writeMetadataFragment() error {
	if len(s.filesMetadata) == 0 {
		return nil
	}

	// 构建元数据片段
	fragment := DatasetFragment{
		ID:          s.metadataConfig.DatasetID,
		Datasource:  s.metadataConfig.Datasource,
		Domain:      s.metadataConfig.Domain,
		Category:    s.metadataConfig.Category,
		Description: s.metadataConfig.Description,
		Granularity: s.metadataConfig.Granularity,
		Coverage:    s.metadataConfig.Coverage,
		Schema:      s.metadataConfig.Schema,
		Files:       s.filesMetadata,
	}

	// 计算存储统计
	var totalSize int64
	var totalRecords int64
	for _, f := range s.filesMetadata {
		totalSize += f.SizeBytes
		totalRecords += f.RecordCount
	}

	fragment.Storage = map[string]any{
		"base_path":        s.outputDir,
		"file_pattern":     fmt.Sprintf("%s_{seq}.%s", s.filenamePrefix, s.outputFormat),
		"compression":      "none",
		"total_files":      len(s.filesMetadata),
		"total_size_bytes": totalSize,
		"total_records":    totalRecords,
	}

	// 构建元数据
	fragment.Metadata = map[string]any{
		"created_at": time.Now().UTC().Format(time.RFC3339),
		"updated_at": time.Now().UTC().Format(time.RFC3339),
		"version":    "1.0",
	}
	if len(s.metadataConfig.Tags) > 0 {
		fragment.Metadata["tags"] = s.metadataConfig.Tags
	}
	if s.metadataConfig.CustomMeta != nil {
		for k, v := range s.metadataConfig.CustomMeta {
			if k != "calculate_checksum" {
				fragment.Metadata[k] = v
			}
		}
	}

	// 确定 pending 目录路径（runtime/data/.metadata/pending）
	pendingDir := filepath.Join("runtime", "data", ".metadata", "pending")
	if err := os.MkdirAll(pendingDir, 0755); err != nil {
		return fmt.Errorf("create pending dir: %w", err)
	}

	// 生成文件名：{datasource}-{category}-{timestamp}.yaml
	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("%s-%s-%s.yaml", s.metadataConfig.Datasource, s.metadataConfig.Category, timestamp)
	fragmentPath := filepath.Join(pendingDir, filename)

	// 序列化并写入
	data, err := yaml.Marshal(fragment)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	// 添加注释头
	header := fmt.Sprintf("# 自动生成的元数据片段\n# 生成时间: %s\n# 数据集: %s\n\n",
		time.Now().UTC().Format(time.RFC3339),
		fragment.ID)

	if err := os.WriteFile(fragmentPath, []byte(header+string(data)), 0644); err != nil {
		return fmt.Errorf("write metadata file: %w", err)
	}

	return nil
}
