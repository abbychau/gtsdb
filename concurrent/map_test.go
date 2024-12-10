package concurrent

import (
	"testing"
)

func TestMap(t *testing.T) {
	t.Run("Store and Load", func(t *testing.T) {
		m := NewMap[string, int]()

		m.Store("key1", 1)
		val, exists := m.Load("key1")

		if !exists {
			t.Error("key should exist")
		}
		if val != 1 {
			t.Errorf("expected 1, got %v", val)
		}
	})

	t.Run("Load non-existent", func(t *testing.T) {
		m := NewMap[string, int]()

		val, exists := m.Load("missing")
		if exists {
			t.Error("key should not exist")
		}
		if val != 0 {
			t.Errorf("expected 0, got %v", val)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		m := NewMap[string, int]()

		m.Store("key1", 1)
		m.Delete("key1")

		_, exists := m.Load("key1")
		if exists {
			t.Error("key should not exist after delete")
		}
	})

	t.Run("LoadOrStore", func(t *testing.T) {
		m := NewMap[string, int]()

		val, loaded := m.LoadOrStore("key1", 1)
		if loaded {
			t.Error("should not be loaded initially")
		}
		if val != 1 {
			t.Errorf("expected 1, got %v", val)
		}

		val, loaded = m.LoadOrStore("key1", 2)
		if !loaded {
			t.Error("should be loaded on second attempt")
		}
		if val != 1 {
			t.Errorf("expected 1, got %v", val)
		}
	})

	t.Run("Range", func(t *testing.T) {
		m := NewMap[string, int]()
		input := map[string]int{"a": 1, "b": 2, "c": 3}

		for k, v := range input {
			m.Store(k, v)
		}

		count := 0
		m.Range(func(k string, v int) bool {
			if input[k] != v {
				t.Errorf("expected %v, got %v for key %v", input[k], v, k)
			}
			count++
			return true
		})

		if count != len(input) {
			t.Errorf("expected %v iterations, got %v", len(input), count)
		}
	})

	t.Run("Clear and Size", func(t *testing.T) {
		m := NewMap[string, int]()

		m.Store("a", 1)
		m.Store("b", 2)

		if size := m.Size(); size != 2 {
			t.Errorf("expected size 2, got %v", size)
		}

		m.Clear()

		if size := m.Size(); size != 0 {
			t.Errorf("expected size 0 after clear, got %v", size)
		}
	})
}
