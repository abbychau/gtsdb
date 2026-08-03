package synchronous

import "sync"

type RingBuffer[T any] struct {
	mu     sync.RWMutex
	buffer []T
	size   int
	head   int
	tail   int
}

func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
	if capacity < 1 {
		capacity = 1 // avoid modulo-by-zero panics in Push
	}
	return &RingBuffer[T]{
		buffer: make([]T, capacity),
		size:   0,
		head:   0,
		tail:   0,
	}
}

func (rb *RingBuffer[T]) Push(item T) {
	rb.mu.Lock()
	rb.buffer[rb.tail] = item
	rb.tail = (rb.tail + 1) % len(rb.buffer)
	if rb.size < len(rb.buffer) {
		rb.size++
	} else {
		rb.head = (rb.head + 1) % len(rb.buffer)
	}
	rb.mu.Unlock()
}

func (rb *RingBuffer[T]) Get(index int) (T, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	if index < 0 || index >= rb.size {
		var zero T
		return zero, false
	}
	pos := (rb.head + index) % len(rb.buffer)
	return rb.buffer[pos], true
}

func (rb *RingBuffer[T]) GetAll() []T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	result := make([]T, rb.size)
	for i := 0; i < rb.size; i++ {
		pos := (rb.head + i) % len(rb.buffer)
		result[i] = rb.buffer[pos]
	}
	return result
}

func (rb *RingBuffer[T]) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

func (rb *RingBuffer[T]) Capacity() int {
	return len(rb.buffer)
}

// GetLast returns the last n items in insertion order with a single lock acquisition.
// If n > size, returns all items. Returns nil if n <= 0.
func (rb *RingBuffer[T]) GetLast(n int) []T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	if n <= 0 || rb.size == 0 {
		return nil
	}
	if n > rb.size {
		n = rb.size
	}
	result := make([]T, n)
	// The last n items start at (head + size - n) % capacity
	start := (rb.head + rb.size - n) % len(rb.buffer)
	for i := 0; i < n; i++ {
		result[i] = rb.buffer[(start+i)%len(rb.buffer)]
	}
	return result
}
