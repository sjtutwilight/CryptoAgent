package generator

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"mock-service/internal/config"
	"mock-service/internal/model"
	"sort"
	"sync"
	"time"
)

// BinanceOrderBookSimulator 模拟Binance订单簿快照与增量
type BinanceOrderBookSimulator struct {
	cfg    *config.BinanceConfig
	books  map[string]*orderBookState
	trades map[string]*tradeState
	mu     sync.RWMutex
	closed chan struct{}
}

// NewBinanceOrderBookSimulator 创建新的订单簿模拟器
func NewBinanceOrderBookSimulator(cfg *config.BinanceConfig) *BinanceOrderBookSimulator {
	if cfg == nil || !cfg.Enabled {
		return nil
	}

	sim := &BinanceOrderBookSimulator{
		cfg:    cfg,
		books:  make(map[string]*orderBookState),
		trades: make(map[string]*tradeState),
		closed: make(chan struct{}),
	}

	interval := time.Duration(cfg.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}

	for _, symbolCfg := range cfg.Symbols {
		if symbolCfg.Symbol == "" {
			continue
		}
		book := newOrderBookState(symbolCfg, interval)
		sim.books[symbolCfg.Symbol] = book

		// 创建交易状态 (频率低于订单簿更新，适合调试)
		tradeInterval := interval * 5 // 交易频率是订单簿更新的1/5
		trade := newTradeState(symbolCfg, tradeInterval)
		sim.trades[symbolCfg.Symbol] = trade
	}

	return sim
}

// Start 启动所有订单簿模拟
func (s *BinanceOrderBookSimulator) Start() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, book := range s.books {
		book.start()
	}
	for _, trade := range s.trades {
		trade.start()
	}
}

// Stop 停止模拟器
func (s *BinanceOrderBookSimulator) Stop() {
	select {
	case <-s.closed:
		return
	default:
		close(s.closed)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, book := range s.books {
		book.stop()
	}
	for _, trade := range s.trades {
		trade.stop()
	}
}

// Snapshot 返回深度快照
func (s *BinanceOrderBookSimulator) Snapshot(symbol string, limit int) (*model.BinanceDepthSnapshot, error) {
	book, err := s.getBook(symbol)
	if err != nil {
		return nil, err
	}

	return book.snapshot(limit), nil
}

// SubscribeDiff 订阅增量数据
func (s *BinanceOrderBookSimulator) SubscribeDiff(symbol string) (int, <-chan model.BinanceDepthDiff, error) {
	book, err := s.getBook(symbol)
	if err != nil {
		return 0, nil, err
	}

	return book.subscribe()
}

// UnsubscribeDiff 取消增量订阅
func (s *BinanceOrderBookSimulator) UnsubscribeDiff(symbol string, id int) {
	book, err := s.getBook(symbol)
	if err != nil {
		return
	}
	book.unsubscribe(id)
}

func (s *BinanceOrderBookSimulator) getBook(symbol string) (*orderBookState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	book, ok := s.books[symbol]
	if !ok {
		return nil, fmt.Errorf("symbol %s not configured", symbol)
	}
	return book, nil
}

// orderBookState 表示单个交易对的订单簿状态
type orderBookState struct {
	cfg          config.BinanceSymbolConfig
	bids         []bookLevel
	asks         []bookLevel
	lastUpdateID int64
	prevFinalID  int64
	mu           sync.RWMutex

	subscribers map[int]chan model.BinanceDepthDiff
	nextSubID   int

	interval time.Duration
	rng      *rand.Rand

	stopCh chan struct{}
	doneCh chan struct{}
}

type bookLevel struct {
	price    float64
	quantity float64
}

func newOrderBookState(cfg config.BinanceSymbolConfig, interval time.Duration) *orderBookState {
	if cfg.Levels <= 0 {
		cfg.Levels = 50
	}
	if cfg.PriceTick <= 0 {
		cfg.PriceTick = 0.5
	}
	if cfg.QuantityTick <= 0 {
		cfg.QuantityTick = 0.01
	}
	if cfg.BasePrice <= 0 {
		cfg.BasePrice = 100
	}
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}

	book := &orderBookState{
		cfg:          cfg,
		lastUpdateID: 1000,
		prevFinalID:  1000,
		subscribers:  make(map[int]chan model.BinanceDepthDiff),
		interval:     interval,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
	book.initLevels()
	return book
}

func (o *orderBookState) initLevels() {
	o.bids = make([]bookLevel, o.cfg.Levels)
	o.asks = make([]bookLevel, o.cfg.Levels)

	price := o.cfg.BasePrice
	for i := 0; i < o.cfg.Levels; i++ {
		o.bids[i] = bookLevel{
			price:    truncateFloat(price-float64(i)*o.cfg.PriceTick, 8),
			quantity: truncateFloat(o.randomQuantity(), 8),
		}
		o.asks[i] = bookLevel{
			price:    truncateFloat(price+float64(i+1)*o.cfg.PriceTick, 8),
			quantity: truncateFloat(o.randomQuantity(), 8),
		}
	}
}

func (o *orderBookState) start() {
	go o.loop()
}

func (o *orderBookState) stop() {
	close(o.stopCh)
	<-o.doneCh

	o.mu.Lock()
	for id, ch := range o.subscribers {
		close(ch)
		delete(o.subscribers, id)
	}
	o.mu.Unlock()
}

func (o *orderBookState) loop() {
	ticker := time.NewTicker(o.interval)
	defer func() {
		ticker.Stop()
		close(o.doneCh)
	}()

	for {
		select {
		case <-ticker.C:
			event, ok := o.generateDiff()
			if ok {
				o.broadcast(event)
			}
		case <-o.stopCh:
			return
		}
	}
}

func (o *orderBookState) snapshot(limit int) *model.BinanceDepthSnapshot {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if limit <= 0 || limit > len(o.bids) {
		limit = len(o.bids)
	}
	if limit > len(o.asks) {
		limit = len(o.asks)
	}

	eventTime := time.Now().UnixMilli()

	snapshot := &model.BinanceDepthSnapshot{
		LastUpdateID:    o.lastUpdateID,
		EventTime:       eventTime,
		TransactionTime: eventTime,
		Bids:            make([][]string, 0, limit),
		Asks:            make([][]string, 0, limit),
	}

	for i := 0; i < limit; i++ {
		snapshot.Bids = append(snapshot.Bids, []string{
			floatToString(o.bids[i].price),
			floatToString(o.bids[i].quantity),
		})
		snapshot.Asks = append(snapshot.Asks, []string{
			floatToString(o.asks[i].price),
			floatToString(o.asks[i].quantity),
		})
	}

	return snapshot
}

func (o *orderBookState) subscribe() (int, <-chan model.BinanceDepthDiff, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.subscribers == nil {
		return 0, nil, errors.New("order book not running")
	}

	o.nextSubID++
	id := o.nextSubID
	ch := make(chan model.BinanceDepthDiff, 16)
	o.subscribers[id] = ch
	return id, ch, nil
}

func (o *orderBookState) unsubscribe(id int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ch, ok := o.subscribers[id]; ok {
		close(ch)
		delete(o.subscribers, id)
	}
}

func (o *orderBookState) generateDiff() (model.BinanceDepthDiff, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	changes := o.rng.Intn(3) + 1
	bidIndices := make(map[int]struct{})
	askIndices := make(map[int]struct{})

	for i := 0; i < changes; i++ {
		isBid := o.rng.Float64() < 0.5
		if isBid {
			idx := o.rng.Intn(len(o.bids))
			o.bids[idx].quantity = truncateFloat(o.randomQuantity(), 8)
			bidIndices[idx] = struct{}{}
		} else {
			idx := o.rng.Intn(len(o.asks))
			o.asks[idx].quantity = truncateFloat(o.randomQuantity(), 8)
			askIndices[idx] = struct{}{}
		}
	}

	if len(bidIndices) == 0 && len(askIndices) == 0 {
		return model.BinanceDepthDiff{}, false
	}

	eventTime := time.Now().UnixMilli()
	prev := o.lastUpdateID
	first := prev + 1
	final := prev + int64(len(bidIndices)+len(askIndices))

	o.prevFinalID = prev
	o.lastUpdateID = final

	event := model.BinanceDepthDiff{
		EventType:         "depthUpdate",
		EventTime:         eventTime,
		TransactionTime:   eventTime,
		Symbol:            o.cfg.Symbol,
		FirstUpdateID:     first,
		FinalUpdateID:     final,
		PrevFinalUpdateID: o.prevFinalID,
		Bids:              o.collectLevels(o.bids, bidIndices, true),
		Asks:              o.collectLevels(o.asks, askIndices, false),
	}

	return event, true
}

func (o *orderBookState) broadcast(event model.BinanceDepthDiff) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, ch := range o.subscribers {
		select {
		case ch <- event:
		default:
			// 如果订阅者处理不过来则丢弃最新事件
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- event:
			default:
				// 无法写入则跳过，避免阻塞
			}
		}
	}
}

func (o *orderBookState) collectLevels(levels []bookLevel, indexSet map[int]struct{}, isBid bool) [][]string {
	if len(indexSet) == 0 {
		return [][]string{}
	}

	indices := make([]int, 0, len(indexSet))
	for idx := range indexSet {
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	if isBid {
		// Binance bids 默认从高到低，因此逆序
		for i, j := 0, len(indices)-1; i < j; i, j = i+1, j-1 {
			indices[i], indices[j] = indices[j], indices[i]
		}
	}

	result := make([][]string, 0, len(indices))
	for _, idx := range indices {
		level := levels[idx]
		result = append(result, []string{
			floatToString(level.price),
			floatToString(level.quantity),
		})
	}
	return result
}

func (o *orderBookState) randomQuantity() float64 {
	base := o.cfg.QuantityTick * float64(o.rng.Intn(200)+1)
	return math.Max(base, o.cfg.QuantityTick)
}

func floatToString(val float64) string {
	return fmt.Sprintf("%.8f", val)
}

func truncateFloat(val float64, precision int) float64 {
	pow := math.Pow10(precision)
	return math.Trunc(val*pow) / pow
}

// SubscribeTrade 订阅交易数据
func (s *BinanceOrderBookSimulator) SubscribeTrade(symbol string) (int, <-chan model.BinanceAggTrade, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trade, ok := s.trades[symbol]
	if !ok {
		return 0, nil, fmt.Errorf("symbol %s not configured", symbol)
	}

	return trade.subscribe()
}

// UnsubscribeTrade 取消交易订阅
func (s *BinanceOrderBookSimulator) UnsubscribeTrade(symbol string, id int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trade, ok := s.trades[symbol]
	if !ok {
		return
	}
	trade.unsubscribe(id)
}

// tradeState 表示单个交易对的交易状态
type tradeState struct {
	cfg         config.BinanceSymbolConfig
	lastAggID   int64
	lastTradeID int64
	mu          sync.RWMutex

	subscribers map[int]chan model.BinanceAggTrade
	nextSubID   int

	interval time.Duration
	rng      *rand.Rand

	stopCh chan struct{}
	doneCh chan struct{}
}

func newTradeState(cfg config.BinanceSymbolConfig, interval time.Duration) *tradeState {
	if cfg.BasePrice <= 0 {
		cfg.BasePrice = 100
	}
	if cfg.PriceTick <= 0 {
		cfg.PriceTick = 0.5
	}
	if cfg.QuantityTick <= 0 {
		cfg.QuantityTick = 0.01
	}
	if interval <= 0 {
		interval = 1 * time.Second
	}

	return &tradeState{
		cfg:         cfg,
		lastAggID:   5933000,
		lastTradeID: 100000,
		subscribers: make(map[int]chan model.BinanceAggTrade),
		interval:    interval,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
}

func (t *tradeState) start() {
	go t.loop()
}

func (t *tradeState) stop() {
	close(t.stopCh)
	<-t.doneCh

	t.mu.Lock()
	for id, ch := range t.subscribers {
		close(ch)
		delete(t.subscribers, id)
	}
	t.mu.Unlock()
}

func (t *tradeState) loop() {
	ticker := time.NewTicker(t.interval)
	defer func() {
		ticker.Stop()
		close(t.doneCh)
	}()

	for {
		select {
		case <-ticker.C:
			trade := t.generateTrade()
			t.broadcast(trade)
		case <-t.stopCh:
			return
		}
	}
}

func (t *tradeState) subscribe() (int, <-chan model.BinanceAggTrade, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.subscribers == nil {
		return 0, nil, errors.New("trade state not running")
	}

	t.nextSubID++
	id := t.nextSubID
	ch := make(chan model.BinanceAggTrade, 16)
	t.subscribers[id] = ch
	return id, ch, nil
}

func (t *tradeState) unsubscribe(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ch, ok := t.subscribers[id]; ok {
		close(ch)
		delete(t.subscribers, id)
	}
}

func (t *tradeState) generateTrade() model.BinanceAggTrade {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now().UnixMilli()

	// 生成价格：在基础价格附近波动
	priceVariation := (t.rng.Float64() - 0.5) * t.cfg.BasePrice * 0.02 // +/- 2%
	price := t.cfg.BasePrice + priceVariation
	// BTCUSDT永续合约价格精度为2位小数（0.01 USD tick）
	price = truncateFloat(price, 2)

	// 生成数量
	quantity := t.cfg.QuantityTick * float64(t.rng.Intn(100)+1)
	// BTCUSDT永续合约数量精度为3位小数（0.001 BTC tick）
	quantity = truncateFloat(quantity, 3)

	// 聚合交易包含1-5笔交易
	numTrades := int64(t.rng.Intn(5) + 1)
	firstTradeID := t.lastTradeID + 1
	lastTradeID := firstTradeID + numTrades - 1
	t.lastTradeID = lastTradeID

	t.lastAggID++

	// 随机决定买方是否是做市商
	isBuyerMaker := t.rng.Float64() < 0.5

	return model.BinanceAggTrade{
		EventType:    "aggTrade",
		EventTime:    now,
		Symbol:       t.cfg.Symbol,
		AggTradeID:   t.lastAggID,
		Price:        floatToString(price),
		Quantity:     floatToString(quantity),
		FirstTradeID: firstTradeID,
		LastTradeID:  lastTradeID,
		TradeTime:    now,
		IsBuyerMaker: isBuyerMaker,
	}
}

func (t *tradeState) broadcast(trade model.BinanceAggTrade) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, ch := range t.subscribers {
		select {
		case ch <- trade:
		default:
			// 如果订阅者处理不过来则丢弃最新事件
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- trade:
			default:
				// 无法写入则跳过，避免阻塞
			}
		}
	}
}
