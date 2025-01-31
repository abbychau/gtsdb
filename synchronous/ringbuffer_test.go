package synchronous

import (
	"testing"
)

func TestRingBuffer_BasicOperations(t *testing.T) {
	rb := NewRingBuffer[int](3)

	// Test initial state
	if rb.Size() != 0 {
		t.Errorf("Expected size 0, got %d", rb.Size())
	}

	// Test pushing elements
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)

	if rb.Size() != 3 {
		t.Errorf("Expected size 3, got %d", rb.Size())
	}

	// Test getting elements
	if v := rb.Get(0); v != 1 {
		t.Errorf("Expected 1, got %d", v)
	}
	if v := rb.Get(2); v != 3 {
		t.Errorf("Expected 3, got %d", v)
	}
}

func TestRingBuffer_Overflow(t *testing.T) {
	rb := NewRingBuffer[int](3)

	// Fill buffer and overflow
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)
	rb.Push(4) // Should override oldest element (1)

	if rb.Size() != 3 {
		t.Errorf("Expected size 3, got %d", rb.Size())
	}

	if v := rb.Get(0); v != 2 {
		t.Errorf("Expected 2, got %d", v)
	}
}

func TestRingBuffer_GetAll(t *testing.T) {
	rb := NewRingBuffer[int](3)
	rb.Push(1)
	rb.Push(2)

	values := rb.GetAll()
	if len(values) != 2 {
		t.Errorf("Expected length 2, got %d", len(values))
	}
	if values[0] != 1 || values[1] != 2 {
		t.Error("Values not retrieved correctly")
	}
}

func TestRingBuffer_PanicOnInvalidGet(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic on invalid Get")
		}
	}()

	rb := NewRingBuffer[int](3)
	rb.Get(0) // Should panic
}

// Benchmarks
func BenchmarkRingBuffer_Push(b *testing.B) {
	rb := NewRingBuffer[int](1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Push(i)
	}
}

func BenchmarkRingBuffer_Get(b *testing.B) {
	rb := NewRingBuffer[int](1000)
	for i := 0; i < 1000; i++ {
		rb.Push(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Get(i % rb.Size())
	}
}

func BenchmarkRingBuffer_GetAll(b *testing.B) {
	rb := NewRingBuffer[int](1000)
	for i := 0; i < 1000; i++ {
		rb.Push(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.GetAll()
	}
}

func BenchmarkRingBuffer_PushWithOverflow(b *testing.B) {
	rb := NewRingBuffer[int](100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Push(i)
	}
}

func TestRingBuffer_Capacity(t *testing.T) {
	// Test initial capacity
	capacities := []int{1, 5, 10, 100}
	for _, cap := range capacities {
		rb := NewRingBuffer[int](cap)
		if rb.Capacity() != cap {
			t.Errorf("Expected capacity %d, got %d", cap, rb.Capacity())
		}
	}

	// Test capacity remains constant after operations
	rb := NewRingBuffer[int](3)
	initialCap := rb.Capacity()

	// After pushes
	rb.Push(1)
	rb.Push(2)
	if rb.Capacity() != initialCap {
		t.Errorf("Capacity changed after push: expected %d, got %d", initialCap, rb.Capacity())
	}

	// After overflow
	rb.Push(3)
	rb.Push(4)
	if rb.Capacity() != initialCap {
		t.Errorf("Capacity changed after overflow: expected %d, got %d", initialCap, rb.Capacity())
	}

	// After gets
	rb.Get(0)
	rb.GetAll()
	if rb.Capacity() != initialCap {
		t.Errorf("Capacity changed after gets: expected %d, got %d", initialCap, rb.Capacity())
	}
}
