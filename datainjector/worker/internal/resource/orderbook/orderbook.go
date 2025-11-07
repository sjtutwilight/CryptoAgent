package orderbook

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrNoSnapshot   = errors.New("orderbook: snapshot not initialized")
	ErrStaleUpdate  = errors.New("orderbook: stale update ignored")
	ErrSequenceGap  = errors.New("orderbook: sequence gap detected")
	ErrInvalidInput = errors.New("orderbook: invalid input")
)

// Level represents a single price level in the orderbook.
type Level struct {
	Price string
	Size  string
}

// Snapshot describes a full orderbook snapshot response.
type Snapshot struct {
	LastUpdateID int64
	Bids         []Level
	Asks         []Level
}

// Diff describes an incremental update from the exchange stream.
type Diff struct {
	FirstUpdateID int64
	FinalUpdateID int64
	PrevFinalID   int64
	Bids          []Level
	Asks          []Level
}

// Book is the normalized orderbook view emitted to downstream handlers.
type Book struct {
	Bids     [][]string
	Asks     [][]string
	Seq      int64
	Snapshot bool
}

// BookState maintains an in-memory orderbook.
type BookState struct {
	mu           sync.Mutex
	lastUpdateID int64             // 最近应用的 update id
	maxDepth     int               // 对外输出的深度上限
	bids         map[string]string // price -> size
	asks         map[string]string
}

// NewBookState constructs an empty orderbook state with a depth cap.
func NewBookState(maxDepth int) *BookState {
	if maxDepth <= 0 {
		maxDepth = 200
	}
	return &BookState{
		maxDepth: maxDepth,
		bids:     make(map[string]string),
		asks:     make(map[string]string),
	}
}

// Reset clears the orderbook state.
func (s *BookState) Reset() {
	s.mu.Lock()
	s.lastUpdateID = 0
	s.bids = make(map[string]string)
	s.asks = make(map[string]string)
	s.mu.Unlock()
}

// LastUpdateID exposes the latest applied update id.
func (s *BookState) LastUpdateID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastUpdateID
}

// ApplySnapshot replaces the current state with the full snapshot.
func (s *BookState) ApplySnapshot(snapshot Snapshot) (Book, error) {
	if snapshot.LastUpdateID == 0 {
		return Book{}, ErrInvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastUpdateID = snapshot.LastUpdateID
	s.bids = make(map[string]string, len(snapshot.Bids))
	s.asks = make(map[string]string, len(snapshot.Asks))

	for _, lv := range snapshot.Bids {
		size := normalizeDecimal(lv.Size)
		if isZeroDecimal(size) {
			continue
		}
		price := normalizeDecimal(lv.Price)
		if price == "" {
			continue
		}
		s.bids[price] = size
	}
	for _, lv := range snapshot.Asks {
		size := normalizeDecimal(lv.Size)
		if isZeroDecimal(size) {
			continue
		}
		price := normalizeDecimal(lv.Price)
		if price == "" {
			continue
		}
		s.asks[price] = size
	}

	return s.buildBookLocked(true), nil
}

// ApplyDiff merges an incremental update into the state.
// Returns:
//   - Book: current view if the diff is applied
//   - bool: true if update applied, false if skipped (stale/out-of-range)
//   - error: non-nil for fatal issues (e.g. gap)
func (s *BookState) ApplyDiff(diff Diff) (Book, bool, error) {
	if diff.FinalUpdateID == 0 {
		return Book{}, false, ErrInvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果快照未初始化，返回 NoSnapshot 错误（integrity handler 会缓存）
	if s.lastUpdateID == 0 {
		return Book{}, false, ErrNoSnapshot
	}

	// Ignore stale updates
	if diff.FinalUpdateID <= s.lastUpdateID {
		return Book{}, false, ErrStaleUpdate
	}

	// Binance 规范：U <= lastUpdateID+1 <= u
	expected := s.lastUpdateID + 1
	if diff.FirstUpdateID > expected {
		return Book{}, false, ErrSequenceGap
	}
	if expected > diff.FinalUpdateID {
		return Book{}, false, ErrStaleUpdate
	}

	// 只有当完全连续（U == lastUpdateID+1）时才检查 pu
	// 如果 U < lastUpdateID+1，说明 diff 跨越了快照点，pu 可能不匹配
	if diff.FirstUpdateID == expected && diff.PrevFinalID != 0 && diff.PrevFinalID != s.lastUpdateID {
		return Book{}, false, ErrSequenceGap
	}

	s.applyDiffLocked(diff)
	return s.buildBookLocked(false), true, nil
}

// applyDiffLocked applies a diff to the internal state (caller must hold lock)
func (s *BookState) applyDiffLocked(diff Diff) {
	for _, lv := range diff.Bids {
		price := normalizeDecimal(lv.Price)
		if price == "" {
			continue
		}
		size := normalizeDecimal(lv.Size)
		if isZeroDecimal(size) {
			// 交易所语义：size == 0 代表删除该档位
			delete(s.bids, price)
			continue
		}
		s.bids[price] = size
	}
	for _, lv := range diff.Asks {
		price := normalizeDecimal(lv.Price)
		if price == "" {
			continue
		}
		size := normalizeDecimal(lv.Size)
		if isZeroDecimal(size) {
			delete(s.asks, price)
			continue
		}
		s.asks[price] = size
	}
	s.lastUpdateID = diff.FinalUpdateID
}

func (s *BookState) buildBookLocked(snapshot bool) Book {
	return Book{
		Bids:     s.sortedLevels(s.bids, true),
		Asks:     s.sortedLevels(s.asks, false),
		Seq:      s.lastUpdateID,
		Snapshot: snapshot,
	}
}

func (s *BookState) sortedLevels(levels map[string]string, desc bool) [][]string {
	n := len(levels)
	if n == 0 {
		return [][]string{}
	}
	type level struct {
		price string
		size  string
		sort  float64
	}
	out := make([]level, 0, n)
	for price, size := range levels {
		val, err := strconv.ParseFloat(price, 64)
		if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
			// 异常价格直接丢弃，防止污染排序
			continue
		}
		out = append(out, level{price: price, size: size, sort: val})
	}
	if len(out) == 0 {
		return [][]string{}
	}
	sort.Slice(out, func(i, j int) bool {
		if desc {
			return out[i].sort > out[j].sort
		}
		return out[i].sort < out[j].sort
	})

	limit := s.maxDepth
	if limit <= 0 || limit > len(out) {
		limit = len(out)
	}
	result := make([][]string, 0, limit)
	for idx := 0; idx < limit; idx++ {
		result = append(result, []string{out[idx].price, out[idx].size})
	}
	return result
}

func normalizeDecimal(value string) string {
	if value == "" {
		return ""
	}
	return strings.TrimSpace(value)
}

func isZeroDecimal(value string) bool {
	if value == "" {
		return true
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return false
	}
	return math.Abs(v) < 1e-12
}
