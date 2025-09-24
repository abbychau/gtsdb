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

	t.Run("NewLRUWithEvict callback", func(t *testing.T) {
		evictedKeys := make([]string, 0)
		evictedValues := make([]int, 0)
		
		lru := NewLRUWithEvict[string, int](2, func(key string, value int) {
			evictedKeys = append(evictedKeys, key)
			evictedValues = append(evictedValues, value)
		})

		// Fill cache
		lru.Put("a", 1)
		lru.Put("b", 2)

		// Should have no evictions yet
		if len(evictedKeys) != 0 {
			t.Errorf("expected no evictions, got %d", len(evictedKeys))
		}

		// Add third item, should evict first
		lru.Put("c", 3)

		// Should have one eviction
		if len(evictedKeys) != 1 || evictedKeys[0] != "a" || evictedValues[0] != 1 {
			t.Errorf("expected eviction of 'a', 1, got %v, %v", evictedKeys, evictedValues)
		}
	})

	t.Run("Delete function", func(t *testing.T) {
		lru := NewLRU[string, int](5)

		// Test delete from empty cache
		lru.Delete("nonexistent")
		if lru.Len() != 0 {
			t.Errorf("expected empty cache after deleting nonexistent key")
		}

		// Add some items to test all delete scenarios
		lru.Put("a", 1)
		lru.Put("b", 2)
		lru.Put("c", 3)
		lru.Put("d", 4)
		lru.Put("e", 5)

		// At this point order should be: a(tail) <- b <- c <- d <- e(head)
		// Let's access 'a' to move it from tail position
		lru.Get("a") // now order: b(tail) <- c <- d <- e <- a(head)

		// Delete middle item (c) - this should hit the default case in switch
		lru.Delete("c")
		if lru.Len() != 4 {
			t.Errorf("expected length 4 after deletion, got %d", lru.Len())
		}
		if _, ok := lru.Get("c"); ok {
			t.Error("expected 'c' to be deleted")
		}

		// Delete head (a)
		lru.Delete("a")
		if lru.Len() != 3 {
			t.Errorf("expected length 3 after deleting head, got %d", lru.Len())
		}
		if _, ok := lru.Get("a"); ok {
			t.Error("expected 'a' to be deleted")
		}

		// Delete tail (b)
		lru.Delete("b")
		if lru.Len() != 2 {
			t.Errorf("expected length 2 after deleting tail, got %d", lru.Len())
		}
		if _, ok := lru.Get("b"); ok {
			t.Error("expected 'b' to be deleted")
		}

		// Delete remaining items
		lru.Delete("d")
		lru.Delete("e")
		if lru.Len() != 0 {
			t.Errorf("expected empty cache after deleting all items, got %d", lru.Len())
		}

		// Test delete single item cache (head == tail case)
		lru.Put("single", 1)
		lru.Delete("single")
		if lru.Len() != 0 {
			t.Errorf("expected empty cache after deleting single item")
		}
	})

	t.Run("Range function", func(t *testing.T) {
		lru := NewLRU[string, int](3)

		// Test range on empty cache
		count := 0
		lru.Range(func(key string, value int) bool {
			count++
			return true
		})
		if count != 0 {
			t.Errorf("expected 0 iterations on empty cache, got %d", count)
		}

		// Add items
		lru.Put("a", 1)
		lru.Put("b", 2)
		lru.Put("c", 3)

		// Test full range
		visited := make(map[string]int)
		lru.Range(func(key string, value int) bool {
			visited[key] = value
			return true
		})

		if len(visited) != 3 {
			t.Errorf("expected to visit 3 items, visited %d", len(visited))
		}
		for k, v := range map[string]int{"a": 1, "b": 2, "c": 3} {
			if visited[k] != v {
				t.Errorf("expected %s:%d in visited items", k, v)
			}
		}

		// Test early termination
		visitCount := 0
		lru.Range(func(key string, value int) bool {
			visitCount++
			return visitCount < 2 // stop after 2 items
		})
		if visitCount != 2 {
			t.Errorf("expected early termination at 2 items, got %d", visitCount)
		}
	})

	t.Run("capacity edge cases", func(t *testing.T) {
		// Test capacity < 1 gets normalized to 1
		lru := NewLRU[string, int](0)
		lru.Put("a", 1)
		lru.Put("b", 2) // should evict "a"
		
		if lru.Len() != 1 {
			t.Errorf("expected capacity 1 cache to have length 1, got %d", lru.Len())
		}
		if _, ok := lru.Get("a"); ok {
			t.Error("expected 'a' to be evicted in capacity 1 cache")
		}
		if v, ok := lru.Get("b"); !ok || v != 2 {
			t.Errorf("expected 'b' with value 2, got %v, exists: %v", v, ok)
		}

		// Test NewLRUWithEvict with capacity < 1
		lruWithEvict := NewLRUWithEvict[string, int](-5, nil)
		lruWithEvict.Put("x", 10)
		lruWithEvict.Put("y", 20) // should evict "x"
		
		if lruWithEvict.Len() != 1 {
			t.Errorf("expected capacity 1 cache to have length 1, got %d", lruWithEvict.Len())
		}
	})
}
