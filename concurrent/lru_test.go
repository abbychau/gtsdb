package concurrent

import (
	"sync"
	"testing"
)

func TestLRU(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		lru := NewLRU[string, int](3)

		// Test empty cache
		if _, ok := lru.Get("nonexistent"); ok {
			t.Error("expected miss for nonexistent key")
		}

		// Test put and get
		lru.Put("a", 1)
		if v, ok := lru.Get("a"); !ok || v != 1 {
			t.Errorf("expected 1, got %v, exists: %v", v, ok)
		}
	})

	t.Run("capacity and eviction", func(t *testing.T) {
		lru := NewLRU[string, int](3)

		// Fill cache
		lru.Put("a", 1)
		lru.Put("b", 2)
		lru.Put("c", 3)

		// Verify size
		if lru.Len() != 3 {
			t.Errorf("expected length 3, got %d", lru.Len())
		}

		// Add one more, should evict oldest
		lru.Put("d", 4)
		if _, ok := lru.Get("a"); ok {
			t.Error("expected 'a' to be evicted")
		}
		if v, ok := lru.Get("d"); !ok || v != 4 {
			t.Error("expected 'd' to be present")
		}
	})

	t.Run("update existing", func(t *testing.T) {
		lru := NewLRU[string, int](3)

		lru.Put("a", 1)
		lru.Put("a", 2)

		if v, _ := lru.Get("a"); v != 2 {
			t.Errorf("expected updated value 2, got %d", v)
		}
		if lru.Len() != 1 {
			t.Errorf("expected length 1, got %d", lru.Len())
		}
	})

	t.Run("concurrent access", func(t *testing.T) {
		lru := NewLRU[int, int](100)
		var wg sync.WaitGroup

		// Concurrent writes
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				lru.Put(val, val)
			}(i)
		}

		// Concurrent reads
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				lru.Get(val)
			}(i)
		}

		wg.Wait()
		if lru.Len() > 100 {
			t.Errorf("cache exceeded capacity, size: %d", lru.Len())
		}
	})
}
