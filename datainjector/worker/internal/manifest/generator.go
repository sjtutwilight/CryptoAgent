package manifest

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Generator Manifest 生成器
type Generator struct {
	config     *ManifestFieldConfig
	outputDir  string
	taskID     string
	dataSource string
	createdAt  time.Time
}

// NewGenerator 创建 Manifest 生成器
func NewGenerator(config *ManifestFieldConfig, outputDir, taskID, dataSource string, createdAt time.Time) *Generator {
	return &Generator{
		config:     config,
		outputDir:  outputDir,
		taskID:     taskID,
		dataSource: dataSource,
		createdAt:  createdAt,
	}
}

// Generate 生成 Manifest 文件
// files: 数据文件列表（相对于 outputDir 的路径）
// totalRecords: 总记录数
// ctx: 用于提取自定义字段的上下文
func (g *Generator) Generate(files []string, totalRecords int64, ctx map[string]any) (*Manifest, error) {
	manifest := &Manifest{
		Version:      g.config.Version,
		TaskID:       g.taskID,
		DataSource:   g.dataSource,
		CreatedAt:    g.createdAt,
		CompletedAt:  time.Now(),
		Status:       "completed",
		TotalRecords: totalRecords,
		TotalFiles:   len(files),
		Files:        make([]FileEntry, 0, len(files)),
	}

	// 计算每个文件的元信息
	for _, filename := range files {
		fullPath := filepath.Join(g.outputDir, filename)
		entry, err := g.buildFileEntry(fullPath, filename)
		if err != nil {
			return nil, fmt.Errorf("build file entry for %s: %w", filename, err)
		}
		manifest.Files = append(manifest.Files, *entry)
	}

	// 提取自定义字段
	if len(g.config.CustomFields) > 0 {
		manifest.CustomFields = g.config.ExtractCustomFields(ctx)
	}

	return manifest, nil
}

// buildFileEntry 构建单个文件的元信息
func (g *Generator) buildFileEntry(fullPath, filename string) (*FileEntry, error) {
	stat, err := os.Stat(fullPath)
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	entry := &FileEntry{
		Filename:  filename,
		SizeBytes: stat.Size(),
	}

	// 计算记录数（针对 JSON Lines 格式）
	if filepath.Ext(filename) == ".json" || filepath.Ext(filename) == ".jsonl" {
		count, err := countJSONLines(fullPath)
		if err == nil {
			entry.RecordCount = count
		}
	}

	// 计算校验和
	if g.config.ChecksumAlgorithm != "none" && g.config.ChecksumAlgorithm != "" {
		checksum, err := g.calculateChecksum(fullPath)
		if err != nil {
			return nil, fmt.Errorf("calculate checksum: %w", err)
		}
		entry.Checksum = checksum
	}

	return entry, nil
}

// calculateChecksum 计算文件校验和
func (g *Generator) calculateChecksum(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	switch g.config.ChecksumAlgorithm {
	case "md5":
		hash := md5.New()
		if _, err := io.Copy(hash, file); err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", hash.Sum(nil)), nil
	case "sha256":
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", hash.Sum(nil)), nil
	default:
		return "", fmt.Errorf("unsupported checksum algorithm: %s", g.config.ChecksumAlgorithm)
	}
}

// countJSONLines 统计 JSON Lines 文件的行数
func countJSONLines(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var count int64
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		count++
	}
	return count, nil
}

// SaveToFile 将 Manifest 保存到文件
func (g *Generator) SaveToFile(manifest *Manifest, filename string) error {
	fullPath := filepath.Join(g.outputDir, filename)
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	if err := os.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("write manifest file: %w", err)
	}

	return nil
}



