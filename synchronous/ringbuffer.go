package synchronous

type RingBuffer[T any] struct {
	buffer []T
	size   int
	head   int
	tail   int
}

func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
	return &RingBuffer[T]{
		buffer: make([]T, capacity),
		size:   0,
		head:   0,
		tail:   0,
	}
}

func (rb *RingBuffer[T]) Push(item T) {
	rb.buffer[rb.tail] = item
	rb.tail = (rb.tail + 1) % len(rb.buffer)
	if rb.size < len(rb.buffer) {
		rb.size++
	} else {
		rb.head = (rb.head + 1) % len(rb.buffer)
	}
}

func (rb *RingBuffer[T]) Get(index int) T {
	if index < 0 || index >= rb.size {
		panic("Index out of bounds")
	}
	pos := (rb.head + index) % len(rb.buffer)
	return rb.buffer[pos]
}

func (rb *RingBuffer[T]) GetAll() []T {
	result := make([]T, rb.size)
	for i := 0; i < rb.size; i++ {
		result[i] = rb.Get(i)
	}
	return result
}

func (rb *RingBuffer[T]) Size() int {
	return rb.size
}

func (rb *RingBuffer[T]) Capacity() int {
	return len(rb.buffer)
}
