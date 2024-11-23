package concurrent

import (
	"sync"
	"testing"
)

func TestNewHashMap(t *testing.T) {
	m := NewHashMap[string, int]()
	if m.Size() != 0 {
		t.Errorf("Expected empty map, got size %d", m.Size())
	}
}

func TestBasicOperations(t *testing.T) {
	m := NewHashMap[string, int]()

	// Test Put and Get
	m.Put("one", 1)
	if val, exists := m.Get("one"); !exists || val != 1 {
		t.Errorf("Expected value 1, got %v", val)
	}

	// Test Contains
	if !m.Contains("one") {
		t.Error("Expected key 'one' to exist")
	}

	// Test Size
	if size := m.Size(); size != 1 {
		t.Errorf("Expected size 1, got %d", size)
	}

	// Test Delete
	m.Delete("one")
	if m.Contains("one") {
		t.Error("Expected key 'one' to be deleted")
	}

	// Test Clear
	m.Put("test", 1)
	m.Clear()
	if m.Size() != 0 {
		t.Error("Expected empty map after Clear")
	}
}

func TestConcurrentOperations(t *testing.T) {
	m := NewHashMap[int, int]()
	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 100

	// Concurrent writes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				m.Put(base+j, base+j)
			}
		}(i * numOperations)
	}
	wg.Wait()

	// Verify size
	expectedSize := numGoroutines * numOperations
	if size := m.Size(); size != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, size)
	}

	// Test concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				if _, exists := m.Get(base + j); !exists {
					t.Errorf("Expected key %d to exist", base+j)
				}
			}
		}(i * numOperations)
	}
	wg.Wait()
}

func TestForEach(t *testing.T) {
	m := NewHashMap[string, int]()
	testData := map[string]int{
		"one":   1,
		"two":   2,
		"three": 3,
	}

	for k, v := range testData {
		m.Put(k, v)
	}

	count := 0
	m.ForEach(func(key string, value int) {
		if expected, ok := testData[key]; !ok || expected != value {
			t.Errorf("ForEach: unexpected key-value pair (%s: %d)", key, value)
		}
		count++
	})

	if count != len(testData) {
		t.Errorf("ForEach: expected %d iterations, got %d", len(testData), count)
	}
}

func TestValues(t *testing.T) {
	m := NewHashMap[string, int]()
	testData := map[string]int{
		"one":   1,
		"two":   2,
		"three": 3,
	}

	for k, v := range testData {
		m.Put(k, v)
	}

	values := m.Values()
	if len(values) != len(testData) {
		t.Errorf("Values: expected %d values, got %d", len(testData), len(values))
	}

	for _, v := range values {
		found := false
		for _, expected := range testData {
			if v == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Values: unexpected value %d", v)
		}
	}
}

// go test -benchmem -run=^$ -bench ^BenchmarkHashMap$ gtsdb/concurrent -benchtime=5s
func BenchmarkHashMap(b *testing.B) {
	m := NewHashMap[string, int]()

	b.Run("Put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Put(string(rune(i)), i)
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Get(string(rune(i)))
		}
	})

	b.Run("ConcurrentPut", func(b *testing.B) {
		m := NewHashMap[int, int]()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Put(i, i)
				i++
			}
		})
	})

	b.Run("ConcurrentGet", func(b *testing.B) {
		m := NewHashMap[int, int]()
		for i := 0; i < 1000; i++ {
			m.Put(i, i)
		}
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Get(i % 1000)
				i++
			}
		})
	})

	b.Run("MixedReadWrite", func(b *testing.B) {
		m := NewHashMap[int, int]()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%2 == 0 {
					m.Put(i, i)
				} else {
					m.Get(i - 1)
				}
				i++
			}
		})
	})
}
