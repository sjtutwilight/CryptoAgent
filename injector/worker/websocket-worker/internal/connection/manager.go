package connection

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"websocket-worker/internal/config"

	"github.com/gorilla/websocket"
)

type ConnectionManager struct {
	config     *config.Config
	conn       *websocket.Conn
	url        string
	headers    http.Header
	mu         sync.RWMutex
	connected  bool
	ctx        context.Context
	cancel     context.CancelFunc
	onMessage  func([]byte)
	onError    func(error)
	retryCount int
}

func NewConnectionManager(cfg *config.Config, url string, headers http.Header) *ConnectionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConnectionManager{
		config:    cfg,
		url:       url,
		headers:   headers,
		ctx:       ctx,
		cancel:    cancel,
		connected: false,
	}
}

func (cm *ConnectionManager) SetMessageHandler(handler func([]byte)) {
	cm.onMessage = handler
}

func (cm *ConnectionManager) SetErrorHandler(handler func(error)) {
	cm.onError = handler
}

func (cm *ConnectionManager) Connect() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.connected {
		return nil
	}

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = time.Duration(cm.config.Connection.Heartbeat.Timeout) * time.Second

	conn, _, err := dialer.Dial(cm.url, cm.headers)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", cm.url, err)
	}

	cm.conn = conn
	cm.connected = true
	cm.retryCount = 0

	log.Printf("Connected to %s", cm.url)

	// Start message reader
	go cm.messageReader()

	// Start heartbeat
	go cm.heartbeat()

	return nil
}

func (cm *ConnectionManager) Disconnect() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.connected {
		return
	}

	cm.cancel()
	if cm.conn != nil {
		cm.conn.Close()
		cm.conn = nil
	}
	cm.connected = false
	log.Printf("Disconnected from %s", cm.url)
}

func (cm *ConnectionManager) IsConnected() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.connected
}

func (cm *ConnectionManager) SendMessage(message []byte) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if !cm.connected || cm.conn == nil {
		return fmt.Errorf("connection not established")
	}

	return cm.conn.WriteMessage(websocket.TextMessage, message)
}

func (cm *ConnectionManager) messageReader() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Message reader panic recovered: %v", r)
			cm.handleConnectionError(fmt.Errorf("panic in message reader: %v", r))
		}
	}()

	for {
		select {
		case <-cm.ctx.Done():
			return
		default:
			if !cm.connected || cm.conn == nil {
				return
			}

			_, message, err := cm.conn.ReadMessage()
			if err != nil {
				cm.handleConnectionError(err)
				return
			}

			if cm.onMessage != nil {
				cm.onMessage(message)
			}
		}
	}
}

func (cm *ConnectionManager) heartbeat() {
	ticker := time.NewTicker(time.Duration(cm.config.Connection.Heartbeat.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			if !cm.connected || cm.conn == nil {
				return
			}

			// Send ping
			err := cm.conn.WriteMessage(websocket.PingMessage, []byte{})
			if err != nil {
				log.Printf("Failed to send ping: %v", err)
				cm.handleConnectionError(err)
				return
			}

			// Set pong handler
			cm.conn.SetPongHandler(func(appData string) error {
				log.Printf("Received pong from %s", cm.url)
				return nil
			})

			// Set read deadline for pong response
			timeout := time.Duration(cm.config.Connection.Heartbeat.Timeout) * time.Second
			cm.conn.SetReadDeadline(time.Now().Add(timeout))
		}
	}
}

func (cm *ConnectionManager) handleConnectionError(err error) {
	log.Printf("Connection error for %s: %v", cm.url, err)

	cm.mu.Lock()
	cm.connected = false
	if cm.conn != nil {
		cm.conn.Close()
		cm.conn = nil
	}
	cm.mu.Unlock()

	if cm.onError != nil {
		cm.onError(err)
	}

	// Attempt reconnection
	go cm.reconnect()
}

func (cm *ConnectionManager) reconnect() {
	maxRetries := cm.config.Connection.Reconnect.MaxRetries
	backoffBase := float64(cm.config.Connection.Reconnect.BackoffBase)
	backoffMax := float64(cm.config.Connection.Reconnect.BackoffMax)

	for cm.retryCount < maxRetries {
		select {
		case <-cm.ctx.Done():
			return
		default:
			cm.retryCount++

			// Calculate exponential backoff with max limit
			backoff := math.Min(backoffBase*math.Pow(2, float64(cm.retryCount-1)), backoffMax)
			log.Printf("Attempting reconnection %d/%d to %s in %.0f seconds",
				cm.retryCount, maxRetries, cm.url, backoff)

			time.Sleep(time.Duration(backoff) * time.Second)

			if err := cm.Connect(); err != nil {
				log.Printf("Reconnection attempt %d failed: %v", cm.retryCount, err)
				continue
			}

			log.Printf("Successfully reconnected to %s", cm.url)
			return
		}
	}

	log.Printf("Max reconnection attempts reached for %s", cm.url)
}

