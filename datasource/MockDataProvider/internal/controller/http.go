package controller

import (
	"encoding/json"
	"fmt"
	"log"
	"mock-service/internal/config"
	"mock-service/internal/fault"
	"mock-service/internal/generator"
	"mock-service/internal/model"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// HTTPController HTTP控制器
type HTTPController struct {
	dataGenerator *generator.DataGenerator
	faultInjector *fault.FaultInjector
	config        *config.Config
}

// NewHTTPController 创建新的HTTP控制器
func NewHTTPController(cfg *config.Config, dataGen *generator.DataGenerator, faultInj *fault.FaultInjector) *HTTPController {
	return &HTTPController{
		dataGenerator: dataGen,
		faultInjector: faultInj,
		config:        cfg,
	}
}

// SetupRoutes 设置HTTP路由
func (c *HTTPController) SetupRoutes(r *gin.Engine) {
	// 以太坊JSON-RPC端点
	r.POST("/", c.handleJSONRPC)
	r.POST("/eth", c.handleJSONRPC)

	// 添加GET请求支持（用于简化测试）
	r.GET("/", c.handleGETRequest)
	r.GET("/eth", c.handleGETRequest)

	// 健康检查端点
	r.GET("/health", c.handleHealth)

	// 故障注入统计端点
	r.GET("/fault/stats", c.handleFaultStats)
	r.POST("/fault/reset", c.handleFaultReset)

	log.Println("HTTP路由设置完成")
}

// handleJSONRPC 处理JSON-RPC请求
func (c *HTTPController) handleJSONRPC(ctx *gin.Context) {

	log.Printf("收到JSON-RPC请求: %s", ctx.Request.Body)
	// 检查是否应该注入HTTP故障
	if shouldInject, statusCode := c.faultInjector.ShouldInjectHTTPFault(); shouldInject {
		log.Printf("注入HTTP故障: %d", statusCode)

		var errorResponse interface{}
		switch statusCode {
		case http.StatusTooManyRequests:
			errorResponse = gin.H{
				"error":   "Too Many Requests",
				"message": "Rate limit exceeded",
			}
		case http.StatusInternalServerError:
			errorResponse = gin.H{
				"error":   "Internal Server Error",
				"message": "Server encountered an error",
			}
		case http.StatusServiceUnavailable:
			errorResponse = gin.H{
				"error":   "Service Unavailable",
				"message": "Service temporarily unavailable",
			}
		case http.StatusBadRequest:
			errorResponse = gin.H{
				"error":   "Bad Request",
				"message": "Invalid request format",
			}
		case http.StatusNotFound:
			errorResponse = gin.H{
				"error":   "Not Found",
				"message": "Endpoint not found",
			}
		default:
			errorResponse = gin.H{
				"error":   "Unknown Error",
				"message": "An unknown error occurred",
			}
		}

		ctx.JSON(statusCode, errorResponse)
		return
	}

	var request model.JSONRPCRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		log.Printf("解析JSON-RPC请求失败: %v", err)
		c.sendJSONRPCError(ctx, nil, -32700, "Parse error")
		return
	}

	log.Printf("收到HTTP请求: %s, ID: %v", request.Method, request.ID)

	switch request.Method {
	case "eth_getBlockByNumber":
		c.handleGetBlockByNumber(ctx, &request)
	case "eth_blockNumber":
		c.handleBlockNumber(ctx, &request)
	default:
		c.sendJSONRPCError(ctx, request.ID, -32601, "Method not found")
	}
}

// handleGetBlockByNumber 处理根据区块号获取区块的请求
func (c *HTTPController) handleGetBlockByNumber(ctx *gin.Context, request *model.JSONRPCRequest) {
	var params []interface{}
	if err := json.Unmarshal(request.Params, &params); err != nil || len(params) < 1 {
		c.sendJSONRPCError(ctx, request.ID, -32602, "Invalid params")
		return
	}
	blockNumberStr, ok := params[0].(string)
	if !ok {
		c.sendJSONRPCError(ctx, request.ID, -32602, "Invalid block number")
		return
	}
	var blockNumber int64
	var err error

	if blockNumberStr == "latest" {
		blockNumber = c.dataGenerator.GetCurrentBlockNumber()
	} else if blockNumberStr == "earliest" {
		blockNumber = c.config.Data.Ethereum.StartBlockNumber
	} else if blockNumberStr == "pending" {
		blockNumber = c.dataGenerator.GetCurrentBlockNumber() + 1
	} else {
		// 解析十六进制区块号
		if strings.HasPrefix(blockNumberStr, "0x") {
			blockNumber, err = strconv.ParseInt(blockNumberStr[2:], 16, 64)
		} else {
			blockNumber, err = strconv.ParseInt(blockNumberStr, 10, 64)
		}
		if err != nil {
			c.sendJSONRPCError(ctx, request.ID, -32602, "Invalid block number format")
			return
		}
	}

	block := c.dataGenerator.GetBlockByNumber(blockNumber)
	if block == nil {
		c.sendJSONRPCError(ctx, request.ID, -32000, "Block not found")
		return
	}

	response := model.JSONRPCResponse{
		ID:      request.ID,
		Result:  block,
		JSONRpc: "2.0",
	}

	ctx.JSON(http.StatusOK, response)
	log.Printf("返回区块: %s, 高度: %s", block.Hash, block.Number)
}

// handleBlockNumber 处理获取当前区块号的请求
func (c *HTTPController) handleBlockNumber(ctx *gin.Context, request *model.JSONRPCRequest) {
	blockNumber := c.dataGenerator.GetCurrentBlockNumber()

	response := model.JSONRPCResponse{
		ID:      request.ID,
		Result:  fmt.Sprintf("0x%x", blockNumber),
		JSONRpc: "2.0",
	}

	ctx.JSON(http.StatusOK, response)
	log.Printf("返回当前区块号: %d", blockNumber)
}

// handleHealth 处理健康检查请求
func (c *HTTPController) handleHealth(ctx *gin.Context) {
	health := gin.H{
		"status":        "healthy",
		"timestamp":     fmt.Sprintf("%d", time.Now().Unix()),
		"current_block": c.dataGenerator.GetCurrentBlockNumber(),
	}

	ctx.JSON(http.StatusOK, health)
}

// handleFaultStats 处理故障注入统计请求
func (c *HTTPController) handleFaultStats(ctx *gin.Context) {
	stats := c.faultInjector.GetStats()
	ctx.JSON(http.StatusOK, gin.H{
		"fault_injection_stats": stats,
	})
}

// handleFaultReset 处理故障注入统计重置请求
func (c *HTTPController) handleFaultReset(ctx *gin.Context) {
	c.faultInjector.ResetStats()
	ctx.JSON(http.StatusOK, gin.H{
		"message": "Fault injection stats reset successfully",
	})
}

// handleGETRequest 处理GET请求，将查询参数转换为JSON-RPC请求
func (c *HTTPController) handleGETRequest(ctx *gin.Context) {
	// 检查是否应该注入HTTP故障
	if shouldInject, statusCode := c.faultInjector.ShouldInjectHTTPFault(); shouldInject {
		log.Printf("注入HTTP故障: %d", statusCode)

		var errorResponse interface{}
		switch statusCode {
		case http.StatusTooManyRequests:
			errorResponse = gin.H{
				"error":   "Too Many Requests",
				"message": "Rate limit exceeded",
			}
		case http.StatusInternalServerError:
			errorResponse = gin.H{
				"error":   "Internal Server Error",
				"message": "Server encountered an error",
			}
		case http.StatusServiceUnavailable:
			errorResponse = gin.H{
				"error":   "Service Unavailable",
				"message": "Service temporarily unavailable",
			}
		case http.StatusBadRequest:
			errorResponse = gin.H{
				"error":   "Bad Request",
				"message": "Invalid request format",
			}
		case http.StatusNotFound:
			errorResponse = gin.H{
				"error":   "Not Found",
				"message": "Endpoint not found",
			}
		default:
			errorResponse = gin.H{
				"error":   "Unknown Error",
				"message": "An unknown error occurred",
			}
		}

		ctx.JSON(statusCode, errorResponse)
		return
	}

	// 从查询参数中获取JSON-RPC请求信息
	method := ctx.Query("method")
	if method == "" {
		method = "eth_getBlockByNumber" // 默认方法
	}

	// 构造JSON-RPC请求
	var request model.JSONRPCRequest
	request.Method = method
	id, _ := json.Marshal(1)

	request.JSONRPC = "2.0"
	request.ID = id

	// 根据方法设置默认参数
	switch method {
	case "eth_getBlockByNumber":
		blockNumber := ctx.Query("block")
		if blockNumber == "" {
			blockNumber = "latest"
		}
		fullTx := ctx.Query("full_tx") == "true"
		params, _ := json.Marshal([]interface{}{blockNumber, fullTx})
		request.Params = json.RawMessage(params)

	case "eth_getBlockByHash":
		blockHash := ctx.Query("hash")
		if blockHash == "" {
			c.sendJSONRPCError(ctx, 1, -32602, "Missing required parameter: hash")
			return
		}
		fullTx := ctx.Query("full_tx") == "true"
		params, _ := json.Marshal([]interface{}{blockHash, fullTx})
		request.Params = json.RawMessage(params)

	case "eth_blockNumber":
		params, _ := json.Marshal([]interface{}{})
		request.Params = json.RawMessage(params)

	default:
		c.sendJSONRPCError(ctx, 1, -32601, "Method not found")
		return
	}

	// 处理请求
	log.Printf("收到HTTP GET请求: %s, ID: %v", request.Method, request.ID)

	switch request.Method {
	case "eth_getBlockByNumber":
		c.handleGetBlockByNumber(ctx, &request)
	case "eth_blockNumber":
		c.handleBlockNumber(ctx, &request)
	default:
		c.sendJSONRPCError(ctx, request.ID, -32601, "Method not found")
	}
}

// sendJSONRPCError 发送JSON-RPC错误响应
func (c *HTTPController) sendJSONRPCError(ctx *gin.Context, id interface{}, code int, message string) {
	response := model.JSONRPCResponse{
		ID:      id,
		Error:   &model.JSONRPCError{Code: code, Message: message},
		JSONRpc: "2.0",
	}

	ctx.JSON(http.StatusOK, response)
}
