package orderbook

// Engine orchestrates snapshot/diff application on top of BookState while
// keeping storage concerns within the orderbook package.
type Engine struct {
	state *BookState
}

// NewEngine wires an engine around a shared BookState.
func NewEngine(symbol string, depth int) *Engine {
	return &Engine{
		state: Shared(symbol, depth),
	}
}

// ApplySnapshot replaces the underlying book state with snapshot.
func (e *Engine) ApplySnapshot(snapshot Snapshot) (Book, error) {
	return e.state.ApplySnapshot(snapshot)
}

// ApplyDiff merges an incremental diff into the underlying book.
func (e *Engine) ApplyDiff(diff Diff) (Book, bool, error) {
	return e.state.ApplyDiff(diff)
}

// LastUpdateID returns the last applied update id.
func (e *Engine) LastUpdateID() int64 {
	return e.state.LastUpdateID()
}

// Reset clears the engine state.
func (e *Engine) Reset() {
	e.state.Reset()
}
