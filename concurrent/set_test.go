package concurrent

import (
	"sync"
	"testing"
)

func TestSet_BasicOperations(t *testing.T) {
	s := NewSet[int]()

	// Test Add and Contains
	s.Add(1)
	s.Add(2)
	s.Add(3)

	if !s.Contains(1) || !s.Contains(2) || !s.Contains(3) {
		t.Error("Set should contain added elements")
	}

	if s.Size() != 3 {
		t.Errorf("Expected size 3, got %d", s.Size())
	}

	// Test Remove
	s.Remove(2)
	if s.Contains(2) {
		t.Error("Set should not contain removed element")
	}

	if s.Size() != 2 {
		t.Errorf("Expected size 2, got %d", s.Size())
	}

	// Test Clear
	s.Clear()
	if s.Size() != 0 {
		t.Error("Set should be empty after clear")
	}
}

func TestSet_ConcurrentOperations(t *testing.T) {
	s := NewSet[int]()
	var wg sync.WaitGroup
	n := 1000

	// Concurrent additions
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(val int) {
			defer wg.Done()
			s.Add(val)
		}(i)
	}
	wg.Wait()

	if s.Size() != n {
		t.Errorf("Expected size %d, got %d", n, s.Size())
	}

	// Concurrent reads
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(val int) {
			defer wg.Done()
			s.Contains(val)
		}(i)
	}
	wg.Wait()
}

func TestSet_SetOperations(t *testing.T) {
	s1 := NewSet[int]()
	s2 := NewSet[int]()

	// Prepare sets
	s1.Add(1)
	s1.Add(2)
	s1.Add(3)

	s2.Add(2)
	s2.Add(3)
	s2.Add(4)

	// Test Union
	union := s1.Union(s2)
	if union.Size() != 4 {
		t.Errorf("Union should have 4 elements, got %d", union.Size())
		//print s1, s2, union
		t.Errorf("s1: %v", s1.Items())
		t.Errorf("s2: %v", s2.Items())
		t.Errorf("union: %v", union.Items())
	}

	// Test Intersection
	intersection := s1.Intersection(s2)
	if intersection.Size() != 2 {
		t.Errorf("Intersection should have 2 elements, got %d", intersection.Size())
	}
}

func TestSet_Items(t *testing.T) {
	s := NewSet[any]()
	items := []any{1, "two", 3.0}

	for _, item := range items {
		s.Add(item)
	}

	result := s.Items()
	if len(result) != len(items) {
		t.Errorf("Expected %d items, got %d", len(items), len(result))
	}
}

func BenchmarkSet_Add(b *testing.B) {
	s := NewSet[int]()
	for i := 0; i < b.N; i++ {
		s.Add(i)
	}
}

func BenchmarkSet_Contains(b *testing.B) {
	s := NewSet[int]()
	for i := 0; i < 1000; i++ {
		s.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Contains(i % 1000)
	}
}

// go test -benchmem -run=^$ -bench ^BenchmarkSet_ConcurrentAdd$ gtsdb/concurrent -benchtime=5s
func BenchmarkSet_ConcurrentAdd(b *testing.B) {
	s := NewSet[int]()
	numWorkers := 100
	var wg sync.WaitGroup
	opsPerWorker := b.N / numWorkers

	b.ResetTimer()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				s.Add(start + j)
			}
		}(i * opsPerWorker)
	}
	wg.Wait()
}
