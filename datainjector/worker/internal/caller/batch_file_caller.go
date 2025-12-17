package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/manifest"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/resource"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// BatchFileCaller 批量文件 Caller，用于分页拉取 REST API 数据并直接写入本地文件
type BatchFileCaller struct {
	// API 配置
	endpoint     string
	pathTemplate string
	headers      map[string]string
	httpConfig   protocol.HTTPClientConfig

	// 分页配置
	pageSize    int
	cursorParam string // 游标参数名，如 "offset"
	cursorField string // 响应中的游标字段名，如 "next_offset"
	dataField   string // 响应中的数据数组字段名，如 "holders"

	// 输出配置
	outputDir         string
	outputFormat      string
	filenamePrefix    string
	maxRecordsPerFile int64

	// Manifest 配置
	manifestConfig *manifest.ManifestFieldConfig

	// 限流器
	rateLimiter *resource.RateLimiter
}

// BatchFileCallerConfig 批量文件 Caller 配置
type BatchFileCallerConfig struct {
	Endpoint     string            `yaml:"endpoint" json:"endpoint"`
	PathTemplate string            `yaml:"path_template" json:"path_template"`
	Headers      map[string]string `yaml:"headers" json:"headers"`
	TimeoutMs    int               `yaml:"timeout_ms" json:"timeout_ms"`

	PageSize    int    `yaml:"page_size" json:"page_size"`
	CursorParam string `yaml:"cursor_param" json:"cursor_param"`
	CursorField string `yaml:"cursor_field" json:"cursor_field"`
	DataField   string `yaml:"data_field" json:"data_field"`

	OutputDir         string `yaml:"output_dir" json:"output_dir"`
	OutputFormat      string `yaml:"output_format" json:"output_format"`
	FilenamePrefix    string `yaml:"filename_prefix" json:"filename_prefix"`
	MaxRecordsPerFile int    `yaml:"max_records_per_file" json:"max_records_per_file"`

	Manifest map[string]any `yaml:"manifest" json:"manifest"`
}

func init() {
	Register("batch_file", func(class string, params map[string]any) (Caller, error) {
		return NewBatchFileCaller(class, params)
	})
}

// NewBatchFileCaller 创建批量文件 Caller
func NewBatchFileCaller(class string, params map[string]any) (*BatchFileCaller, error) {
	callerConfig, _ := params["caller_config"].(map[string]any)
	if callerConfig == nil {
		return nil, fmt.Errorf("caller_config required for batch_file")
	}

	c := &BatchFileCaller{
		pageSize:          500,
		cursorParam:       "offset",
		cursorField:       "next_offset",
		dataField:         "data",
		outputFormat:      "json",
		filenamePrefix:    "data",
		maxRecordsPerFile: 10000,
	}

	// 解析 API 配置
	if endpoint, ok := callerConfig["endpoint"].(string); ok {
		c.endpoint = endpoint
	}
	if c.endpoint == "" {
		return nil, fmt.Errorf("endpoint required")
	}

	if pathTemplate, ok := callerConfig["path_template"].(string); ok {
		c.pathTemplate = pathTemplate
	}
	if c.pathTemplate == "" {
		return nil, fmt.Errorf("path_template required")
	}

	// 解析 Headers
	c.headers = make(map[string]string)
	if headers, ok := callerConfig["headers"].(map[string]any); ok {
		for k, v := range headers {
			if str, ok := v.(string); ok {
				// 支持环境变量替换
				c.headers[k] = os.ExpandEnv(str)
			}
		}
	}

	// HTTP 配置
	timeout := getIntValue(callerConfig, "timeout_ms", 30000)
	c.httpConfig = protocol.HTTPClientConfig{
		TimeoutMs: timeout,
	}

	// 分页配置
	if pageSize := getIntValue(callerConfig, "page_size", 0); pageSize > 0 {
		c.pageSize = pageSize
	}
	if cursorParam, ok := callerConfig["cursor_param"].(string); ok && cursorParam != "" {
		c.cursorParam = cursorParam
	}
	if cursorField, ok := callerConfig["cursor_field"].(string); ok && cursorField != "" {
		c.cursorField = cursorField
	}
	if dataField, ok := callerConfig["data_field"].(string); ok && dataField != "" {
		c.dataField = dataField
	}

	// 输出配置
	if outputDir, ok := callerConfig["output_dir"].(string); ok {
		c.outputDir = outputDir
	}
	if c.outputDir == "" {
		return nil, fmt.Errorf("output_dir required")
	}

	if outputFormat, ok := callerConfig["output_format"].(string); ok && outputFormat != "" {
		c.outputFormat = strings.ToLower(outputFormat)
	}
	if filenamePrefix, ok := callerConfig["filename_prefix"].(string); ok && filenamePrefix != "" {
		c.filenamePrefix = filenamePrefix
	}
	if maxRecords := getIntValue(callerConfig, "max_records_per_file", 0); maxRecords > 0 {
		c.maxRecordsPerFile = int64(maxRecords)
	}

	// Manifest 配置
	if manifestCfg, ok := callerConfig["manifest"].(map[string]any); ok {
		cfg, err := manifest.ParseManifestConfig(manifestCfg)
		if err != nil {
			return nil, fmt.Errorf("parse manifest config: %w", err)
		}
		if err := cfg.ValidateChecksumAlgorithm(); err != nil {
			return nil, fmt.Errorf("invalid checksum algorithm: %w", err)
		}
		c.manifestConfig = cfg
	} else {
		// 默认配置
		c.manifestConfig = &manifest.ManifestFieldConfig{
			Version:           "1.0",
			ChecksumAlgorithm: "md5",
		}
	}

	// 限流器
	if rlCfg, ok := callerConfig["rate_limit"].(map[string]any); ok {
		if conf, err := resource.ParseRateLimitConfig(rlCfg); err != nil {
			log.Printf("[BatchFileCaller] parse rate_limit failed: %v", err)
		} else {
			c.rateLimiter = resource.GetManager().GetOrCreateRateLimiter("batch_file", conf)
		}
	}

	return c, nil
}

// CallOnce 执行批量拉取任务
// args 应包含：
//   - task_id: 任务 ID
//   - chain_id: 链 ID（用于路径替换）
//   - address: Token 地址（用于路径替换）
//   - 其他路径参数
func (c *BatchFileCaller) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	taskID := util.ToString(args["task_id"])
	if taskID == "" {
		return nil, fmt.Errorf("task_id required")
	}

	// 构建输出目录（支持路径参数替换）
	outputDir := c.buildOutputDir(args)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	log.Printf("[BatchFileCaller] 开始批量拉取任务: task_id=%s, output_dir=%s", taskID, outputDir)

	// 检查游标文件，支持断点续传
	cursorFile := filepath.Join(outputDir, ".cursor.json")
	cursor, err := c.loadCursor(cursorFile, taskID)
	if err != nil {
		log.Printf("[BatchFileCaller] 加载游标失败，从头开始: %v", err)
		cursor = nil
	}

	if cursor == nil {
		// 创建新的游标状态
		cursor = &manifest.CursorState{
			TaskID:       taskID,
			FilesWritten: []string{},
			LastUpdated:  time.Now(),
		}
		log.Printf("[BatchFileCaller] 开始新任务")
	} else {
		log.Printf("[BatchFileCaller] 从断点继续: offset=%s, total_records=%d, files=%d",
			cursor.NextOffset, cursor.TotalRecords, len(cursor.FilesWritten))
	}

	// 执行批量拉取
	result, err := c.fetchAll(ctx, args, outputDir, cursor)
	if err != nil {
		// 保存游标以便下次重试
		if saveErr := c.saveCursor(cursorFile, cursor); saveErr != nil {
			log.Printf("[BatchFileCaller] 保存游标失败: %v", saveErr)
		}
		return nil, fmt.Errorf("fetch all: %w", err)
	}

	// 生成 Manifest
	manifestPath := filepath.Join(outputDir, "manifest.json")
	if err := c.generateManifest(outputDir, taskID, args, result); err != nil {
		return nil, fmt.Errorf("generate manifest: %w", err)
	}

	// 删除游标文件
	os.Remove(cursorFile)

	log.Printf("[BatchFileCaller] 任务完成: task_id=%s, total_records=%d, files=%d, manifest=%s",
		taskID, result.TotalRecords, len(result.Files), manifestPath)

	// 返回空消息（数据已直接写入文件）
	return []*types.Message{}, nil
}

// fetchResult 拉取结果
type fetchResult struct {
	TotalRecords int64
	Files        []string
	FirstRecord  map[string]any
	LastRecord   map[string]any
}

// fetchAll 执行完整的分页拉取
func (c *BatchFileCaller) fetchAll(ctx context.Context, args map[string]any, outputDir string, cursor *manifest.CursorState) (*fetchResult, error) {
	result := &fetchResult{
		TotalRecords: cursor.TotalRecords,
		Files:        cursor.FilesWritten,
	}

	nextOffset := cursor.NextOffset
	currentFileIndex := cursor.CurrentFileIndex
	recordsInCurrentFile := cursor.RecordsInCurrentFile

	var currentWriter util.FileWriter
	var err error

	// 如果有未完成的文件，继续写入
	if recordsInCurrentFile > 0 && recordsInCurrentFile < c.maxRecordsPerFile {
		filename := fmt.Sprintf("%s_%03d.%s", c.filenamePrefix, currentFileIndex, c.outputFormat)
		filePath := filepath.Join(outputDir, filename)
		currentWriter, err = util.NewFileWriter(util.FileWriterConfig{
			FilePath: filePath,
			Format:   c.outputFormat,
		})
		if err != nil {
			return nil, fmt.Errorf("open file writer: %w", err)
		}
		defer currentWriter.Close()
	}

	for {
		// 限流
		if c.rateLimiter != nil {
			if err := c.rateLimiter.Wait(ctx); err != nil {
				return nil, fmt.Errorf("rate limit wait: %w", err)
			}
		}

		// 拉取一页数据
		page, err := c.fetchPage(ctx, args, nextOffset)
		if err != nil {
			return nil, fmt.Errorf("fetch page (offset=%s): %w", nextOffset, err)
		}

		if len(page.Records) == 0 {
			break
		}

		// 写入记录
		for _, record := range page.Records {
			// 需要创建新文件
			if currentWriter == nil || recordsInCurrentFile >= c.maxRecordsPerFile {
				if currentWriter != nil {
					currentWriter.Close()
				}

				filename := fmt.Sprintf("%s_%03d.%s", c.filenamePrefix, currentFileIndex, c.outputFormat)
				filePath := filepath.Join(outputDir, filename)
				currentWriter, err = util.NewFileWriter(util.FileWriterConfig{
					FilePath: filePath,
					Format:   c.outputFormat,
				})
				if err != nil {
					return nil, fmt.Errorf("create file writer: %w", err)
				}

				result.Files = append(result.Files, filename)
				recordsInCurrentFile = 0
				currentFileIndex++
			}

			if err := currentWriter.Write(record); err != nil {
				return nil, fmt.Errorf("write record: %w", err)
			}

			recordsInCurrentFile++
			result.TotalRecords++

			// 记录第一条和最后一条
			if result.FirstRecord == nil {
				result.FirstRecord = record
			}
			result.LastRecord = record
		}

		// 更新游标
		cursor.NextOffset = page.NextOffset
		cursor.CurrentFileIndex = currentFileIndex
		cursor.RecordsInCurrentFile = recordsInCurrentFile
		cursor.TotalRecords = result.TotalRecords
		cursor.FilesWritten = result.Files
		cursor.LastUpdated = time.Now()

		// 保存游标
		cursorFile := filepath.Join(outputDir, ".cursor.json")
		if err := c.saveCursor(cursorFile, cursor); err != nil {
			log.Printf("[BatchFileCaller] 保存游标失败: %v", err)
		}

		log.Printf("[BatchFileCaller] 已拉取 %d 条记录，当前文件: %s_%03d.%s",
			result.TotalRecords, c.filenamePrefix, currentFileIndex, c.outputFormat)

		// 检查是否还有下一页
		if page.NextOffset == "" {
			break
		}
		nextOffset = page.NextOffset
	}

	if currentWriter != nil {
		currentWriter.Close()
	}

	return result, nil
}

// pageResult 单页结果
type pageResult struct {
	Records    []map[string]any
	NextOffset string
}

// fetchPage 拉取单页数据
func (c *BatchFileCaller) fetchPage(ctx context.Context, args map[string]any, offset string) (*pageResult, error) {
	// 构建 URL
	path := c.buildPath(args)
	fullURL := c.endpoint + path

	// 构建查询参数
	query := url.Values{}
	query.Set("limit", fmt.Sprintf("%d", c.pageSize))
	if offset != "" {
		query.Set(c.cursorParam, offset)
	}

	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	// 构建请求
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// 设置 Headers
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	// 发送请求
	c.httpConfig.Endpoint = c.endpoint
	client := protocol.GetHTTPClient(c.httpConfig)
	if client == nil {
		return nil, fmt.Errorf("http client 初始化失败")
	}

	respBody, statusCode, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	if statusCode >= 400 {
		return nil, fmt.Errorf("http status %d: %s", statusCode, string(respBody))
	}

	// 解析响应
	var respData map[string]any
	if err := json.Unmarshal(respBody, &respData); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	// 提取数据数组
	records, err := c.extractRecords(respData)
	if err != nil {
		return nil, fmt.Errorf("extract records: %w", err)
	}

	// 提取下一页游标
	nextOffset := c.extractNextOffset(respData)

	return &pageResult{
		Records:    records,
		NextOffset: nextOffset,
	}, nil
}

// extractRecords 从响应中提取记录数组
func (c *BatchFileCaller) extractRecords(respData map[string]any) ([]map[string]any, error) {
	dataVal, ok := respData[c.dataField]
	if !ok {
		return nil, fmt.Errorf("data field %q not found in response", c.dataField)
	}

	dataArray, ok := dataVal.([]any)
	if !ok {
		return nil, fmt.Errorf("data field %q is not an array", c.dataField)
	}

	records := make([]map[string]any, 0, len(dataArray))
	for _, item := range dataArray {
		if record, ok := item.(map[string]any); ok {
			records = append(records, record)
		}
	}

	return records, nil
}

// extractNextOffset 从响应中提取下一页游标
func (c *BatchFileCaller) extractNextOffset(respData map[string]any) string {
	if val, ok := respData[c.cursorField]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// buildPath 构建 API 路径（支持参数替换）
func (c *BatchFileCaller) buildPath(args map[string]any) string {
	path := c.pathTemplate
	for key, val := range args {
		placeholder := "{" + key + "}"
		path = strings.ReplaceAll(path, placeholder, util.ToString(val))
	}
	return path
}

// buildOutputDir 构建输出目录（支持参数替换）
func (c *BatchFileCaller) buildOutputDir(args map[string]any) string {
	dir := c.outputDir
	// 支持在输出目录中使用参数，如 /data/{chain_id}/{address}
	for key, val := range args {
		placeholder := "{" + key + "}"
		dir = strings.ReplaceAll(dir, placeholder, util.ToString(val))
	}
	return dir
}

// loadCursor 加载游标文件
func (c *BatchFileCaller) loadCursor(filePath, taskID string) (*manifest.CursorState, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var cursor manifest.CursorState
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, err
	}

	// 验证任务 ID
	if cursor.TaskID != taskID {
		return nil, fmt.Errorf("cursor task_id mismatch: expected %s, got %s", taskID, cursor.TaskID)
	}

	return &cursor, nil
}

// saveCursor 保存游标文件
func (c *BatchFileCaller) saveCursor(filePath string, cursor *manifest.CursorState) error {
	data, err := json.MarshalIndent(cursor, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

// generateManifest 生成 Manifest 文件
func (c *BatchFileCaller) generateManifest(outputDir, taskID string, args map[string]any, result *fetchResult) error {
	// 构建上下文
	ctx := map[string]any{
		"params":       args,
		"first_record": result.FirstRecord,
		"last_record":  result.LastRecord,
	}

	// 创建生成器
	gen := manifest.NewGenerator(
		c.manifestConfig,
		outputDir,
		taskID,
		"batch_file",
		time.Now(),
	)

	// 生成 Manifest
	m, err := gen.Generate(result.Files, result.TotalRecords, ctx)
	if err != nil {
		return fmt.Errorf("generate manifest: %w", err)
	}

	// 保存到文件
	if err := gen.SaveToFile(m, "manifest.json"); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}

	return nil
}
