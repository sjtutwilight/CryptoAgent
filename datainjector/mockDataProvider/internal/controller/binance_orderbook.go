package controller

import (
	"encoding/json"
	"fmt"
	"log"
	"mock-service/internal/config"
	"mock-service/internal/fault"
	"mock-service/internal/generator"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// BinanceOrderBookController 提供Binance订单簿模拟接口
type BinanceOrderBookController struct {
	cfg           *config.Config
	simulator     *generator.BinanceOrderBookSimulator
	faultInjector *fault.FaultInjector
	upgrader      websocket.Upgrader
}

// NewBinanceOrderBookController 创建控制器
func NewBinanceOrderBookController(cfg *config.Config, sim *generator.BinanceOrderBookSimulator, faultInj *fault.FaultInjector) *BinanceOrderBookController {
	if sim == nil {
		return nil
	}
	return &BinanceOrderBookController{
		cfg:           cfg,
		simulator:     sim,
		faultInjector: faultInj,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// RegisterRoutes 注册HTTP与WebSocket路由
func (c *BinanceOrderBookController) RegisterRoutes(r *gin.Engine) {
	if c == nil {
		return
	}

	r.GET("/fapi/v1/depth", c.handleDepthSnapshot)
	r.GET("/api/v3/depth", c.handleDepthSnapshot)
	r.GET("/ws/binance", c.handleWebSocketSubscribe)
	r.GET("/ws/binance/:stream", c.handleWebSocketStream)
}

func (c *BinanceOrderBookController) handleDepthSnapshot(ctx *gin.Context) {
	if shouldFault, status := c.faultInjector.ShouldInjectHTTPFault(); shouldFault {
		ctx.AbortWithStatusJSON(status, gin.H{
			"code":    status,
			"message": "fault injected",
		})
		return
	}

	symbol := strings.ToUpper(ctx.Query("symbol"))
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": "symbol is required",
		})
		return
	}

	limit := c.cfg.Data.Binance.SnapshotDepth
	if limit <= 0 {
		limit = 200
	}

	if limitParam := ctx.Query("limit"); limitParam != "" {
		if v, err := strconv.Atoi(limitParam); err == nil && v > 0 {
			limit = v
		}
	}

	snapshot, err := c.simulator.Snapshot(symbol, limit)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, snapshot)
}

// handleWebSocketStream 处理不同类型的流（depth或aggTrade）
func (c *BinanceOrderBookController) handleWebSocketStream(ctx *gin.Context) {
	rawStream := ctx.Param("stream")
	if rawStream == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": "stream path is required",
		})
		return
	}

	stream, err := url.PathUnescape(rawStream)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": "invalid stream encoding",
		})
		return
	}

	symbol, streamType := parseStream(stream)
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": "unsupported stream format",
		})
		return
	}

	// 根据流类型分发
	switch streamType {
	case "depth":
		c.handleDepthStream(ctx, symbol)
	case "aggtrade": // 注意：这里用小写，因为parseStream会转换为小写
		c.handleAggTradeStream(ctx, symbol)
	default:
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": fmt.Sprintf("unsupported stream type: %s", streamType),
		})
	}
}

func (c *BinanceOrderBookController) handleDepthStream(ctx *gin.Context, symbol string) {
	conn, err := c.upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		log.Printf("Binance depth stream upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	subID, ch, err := c.simulator.SubscribeDiff(symbol)
	if err != nil {
		log.Printf("Subscribe diff failed: %v", err)
		conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), time.Now().Add(2*time.Second))
		return
	}
	defer c.simulator.UnsubscribeDiff(symbol, subID)

	c.setupHeartbeat(conn)

	// 丢弃读取到的消息，用于保持连接
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for event := range ch {
		if c.faultInjector.ShouldInjectWebSocketDisconnection() {
			log.Printf("injecting websocket disconnection fault for %s", symbol)
			return
		}
		if c.faultInjector.ShouldInjectWebSocketDataLoss() {
			log.Printf("injecting websocket data loss fault for %s", symbol)
			continue
		}

		payload, err := json.Marshal(event)
		if err != nil {
			log.Printf("marshal depth diff failed: %v", err)
			continue
		}

		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			log.Printf("write depth diff failed: %v", err)
			return
		}
	}
}

func (c *BinanceOrderBookController) handleAggTradeStream(ctx *gin.Context, symbol string) {
	conn, err := c.upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		log.Printf("Binance aggTrade stream upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	subID, ch, err := c.simulator.SubscribeTrade(symbol)
	if err != nil {
		log.Printf("Subscribe trade failed: %v", err)
		conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), time.Now().Add(2*time.Second))
		return
	}
	defer c.simulator.UnsubscribeTrade(symbol, subID)

	c.setupHeartbeat(conn)

	// 丢弃读取到的消息，用于保持连接
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	log.Printf("开始推送aggTrade数据: symbol=%s", symbol)

	for trade := range ch {
		if c.faultInjector.ShouldInjectWebSocketDisconnection() {
			log.Printf("注入WebSocket断连故障: symbol=%s", symbol)
			return
		}
		if c.faultInjector.ShouldInjectWebSocketDataLoss() {
			log.Printf("注入数据丢失故障: symbol=%s", symbol)
			continue
		}

		payload, err := json.Marshal(trade)
		if err != nil {
			log.Printf("序列化aggTrade失败: %v", err)
			continue
		}

		log.Printf("推送aggTrade: symbol=%s, agg_id=%d, price=%s, qty=%s, is_buyer_maker=%v",
			trade.Symbol, trade.AggTradeID, trade.Price, trade.Quantity, trade.IsBuyerMaker)

		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			log.Printf("发送aggTrade失败: %v", err)
			return
		}
	}
}

type binanceSubscribeReq struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
	ID     interface{}   `json:"id"`
}

// handleWebSocketSubscribe 兼容Binance /ws + SUBSCRIBE 协议
func (c *BinanceOrderBookController) handleWebSocketSubscribe(ctx *gin.Context) {
	conn, err := c.upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		log.Printf("Binance websocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	c.setupHeartbeat(conn)

	var (
		writeMu       sync.Mutex
		subsMu        sync.Mutex
		depthSubs     = make(map[string]func())
		aggTradeSubs  = make(map[string]func())
		eventOut      = make(chan []byte, 256)
		done          = make(chan struct{})
		doneOnce      sync.Once
		closeDoneChan = func() {
			doneOnce.Do(func() {
				close(done)
			})
		}
	)

	defer closeDoneChan()
	defer func() {
		subsMu.Lock()
		defer subsMu.Unlock()
		for _, fn := range depthSubs {
			fn()
		}
		for _, fn := range aggTradeSubs {
			fn()
		}
	}()

	safeWrite := func(messageType int, payload []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteMessage(messageType, payload)
	}

	sendAck := func(id interface{}) {
		resp := map[string]interface{}{
			"result": nil,
			"id":     id,
		}
		data, err := json.Marshal(resp)
		if err != nil {
			return
		}
		if err := safeWrite(websocket.TextMessage, data); err != nil {
			log.Printf("send subscribe ack failed: %v", err)
		}
	}

	sendError := func(id interface{}, msg string) {
		resp := map[string]interface{}{
			"id":    id,
			"error": msg,
		}
		data, err := json.Marshal(resp)
		if err != nil {
			return
		}
		if err := safeWrite(websocket.TextMessage, data); err != nil {
			log.Printf("send subscribe error failed: %v", err)
		}
	}

	subscribeDepth := func(stream, symbol string) error {
		subID, ch, err := c.simulator.SubscribeDiff(symbol)
		if err != nil {
			return err
		}

		depthSubs[stream] = func() {
			c.simulator.UnsubscribeDiff(symbol, subID)
		}

		go func() {
			for {
				select {
				case <-done:
					return
				case event, ok := <-ch:
					if !ok {
						return
					}
					payload, err := json.Marshal(event)
					if err != nil {
						log.Printf("marshal depth diff failed: %v", err)
						continue
					}
					select {
					case eventOut <- payload:
					case <-done:
						return
					}
				}
			}
		}()

		return nil
	}

	subscribeAggTrade := func(stream, symbol string) error {
		subID, ch, err := c.simulator.SubscribeTrade(symbol)
		if err != nil {
			return err
		}

		aggTradeSubs[stream] = func() {
			c.simulator.UnsubscribeTrade(symbol, subID)
		}

		go func() {
			for {
				select {
				case <-done:
					return
				case trade, ok := <-ch:
					if !ok {
						return
					}
					payload, err := json.Marshal(trade)
					if err != nil {
						log.Printf("marshal aggTrade failed: %v", err)
						continue
					}
					select {
					case eventOut <- payload:
					case <-done:
						return
					}
				}
			}
		}()

		return nil
	}

	go func() {
		defer closeDoneChan()

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var req binanceSubscribeReq
			if err := json.Unmarshal(raw, &req); err != nil {
				sendError(nil, "invalid subscribe request")
				continue
			}

			method := strings.ToUpper(strings.TrimSpace(req.Method))
			switch method {
			case "SUBSCRIBE":
				subsMu.Lock()
				var subscribeErr error
				for _, item := range req.Params {
					stream := strings.TrimSpace(fmt.Sprintf("%v", item))
					if stream == "" {
						continue
					}
					if _, exists := depthSubs[stream]; exists {
						continue
					}
					if _, exists := aggTradeSubs[stream]; exists {
						continue
					}

					symbol, streamType := parseStream(stream)
					if symbol == "" {
						subscribeErr = fmt.Errorf("unsupported stream format: %s", stream)
						break
					}

					switch streamType {
					case "depth":
						if err := subscribeDepth(stream, symbol); err != nil {
							subscribeErr = err
						}
					case "aggtrade":
						if err := subscribeAggTrade(stream, symbol); err != nil {
							subscribeErr = err
						}
					default:
						subscribeErr = fmt.Errorf("unsupported stream type: %s", streamType)
					}

					if subscribeErr != nil {
						break
					}
				}
				subsMu.Unlock()

				if subscribeErr != nil {
					sendError(req.ID, subscribeErr.Error())
					continue
				}
				sendAck(req.ID)

			case "UNSUBSCRIBE":
				subsMu.Lock()
				for _, item := range req.Params {
					stream := strings.TrimSpace(fmt.Sprintf("%v", item))
					if stream == "" {
						continue
					}
					if stop, ok := depthSubs[stream]; ok {
						stop()
						delete(depthSubs, stream)
					}
					if stop, ok := aggTradeSubs[stream]; ok {
						stop()
						delete(aggTradeSubs, stream)
					}
				}
				subsMu.Unlock()
				sendAck(req.ID)

			default:
				sendError(req.ID, "unsupported method")
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		case payload := <-eventOut:
			if c.faultInjector.ShouldInjectWebSocketDisconnection() {
				log.Printf("injecting websocket disconnection fault for binance subscribe stream")
				return
			}
			if c.faultInjector.ShouldInjectWebSocketDataLoss() {
				log.Printf("injecting websocket data loss fault for binance subscribe stream")
				continue
			}
			if err := safeWrite(websocket.TextMessage, payload); err != nil {
				log.Printf("write websocket stream failed: %v", err)
				return
			}
		}
	}
}

// setupHeartbeat 设置心跳处理
func (c *BinanceOrderBookController) setupHeartbeat(conn *websocket.Conn) {
	conn.SetPingHandler(func(appData string) error {
		deadline := time.Now().Add(60 * time.Second)
		conn.SetReadDeadline(deadline)
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
}

// parseStream 解析流名称，返回symbol和streamType
// 例如: "btcusdt@depth" -> "BTCUSDT", "depth"
//
//	"btcusdt@depth@100ms" -> "BTCUSDT", "depth"
//	"btcusdt@aggTrade" -> "BTCUSDT", "aggTrade"
func parseStream(stream string) (string, string) {
	lower := strings.ToLower(stream)
	parts := strings.Split(lower, "@")
	if len(parts) < 2 {
		return "", ""
	}

	symbol := strings.ToUpper(parts[0])
	streamType := parts[1]

	if symbol == "" || streamType == "" {
		return "", ""
	}

	return symbol, streamType
}
