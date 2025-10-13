package queue

import (
    "context"
    "errors"
)

var ErrClosed = errors.New("queue closed")

// BoundedQueue 是一个有界队列，基于带缓冲 chan。
type BoundedQueue[T any] struct {
    ch chan T
}

func NewBounded[T any](cap int) *BoundedQueue[T] {
    if cap <= 0 {
        cap = 1
    }
    return &BoundedQueue[T]{ch: make(chan T, cap)}
}

func (b *BoundedQueue[T]) Enqueue(ctx context.Context, v T) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case b.ch <- v:
        return nil
    }
}

func (b *BoundedQueue[T]) Dequeue(ctx context.Context) (T, error) {
    var zero T
    select {
    case <-ctx.Done():
        return zero, ctx.Err()
    case v, ok := <-b.ch:
        if !ok {
            return zero, ErrClosed
        }
        return v, nil
    }
}

func (b *BoundedQueue[T]) Close() { close(b.ch) }

