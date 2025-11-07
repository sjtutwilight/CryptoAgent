package orderbook

import "sync"

var (
	storeMu sync.Mutex
	store   = map[string]*BookState{}
)

// Shared returns a symbol-scoped BookState, creating it on-demand.
func Shared(symbol string, depth int) *BookState {
	storeMu.Lock()
	defer storeMu.Unlock()
	if depth <= 0 {
		depth = 200
	}
	if state, ok := store[symbol]; ok {
		return state
	}
	state := NewBookState(depth)
	store[symbol] = state
	return state
}

// ResetShared clears the cached state for the symbol.
func ResetShared(symbol string) {
	storeMu.Lock()
	delete(store, symbol)
	storeMu.Unlock()
}
