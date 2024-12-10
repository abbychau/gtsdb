package concurrent

import "sync"

type Map[K comparable, V any] struct {
	internal sync.Map
}

// NewMap creates a new concurrent Map
func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{}
}

// Store sets the value for a key
func (m *Map[K, V]) Store(key K, value V) {
	m.internal.Store(key, value)
}

// Load retrieves the value for a key
func (m *Map[K, V]) Load(key K) (V, bool) {
	value, ok := m.internal.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return value.(V), true
}

// Delete removes a key from the map
func (m *Map[K, V]) Delete(key K) {
	m.internal.Delete(key)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
func (m *Map[K, V]) LoadOrStore(key K, value V) (V, bool) {
	actual, loaded := m.internal.LoadOrStore(key, value)
	return actual.(V), loaded
}

// Range calls f sequentially for each key and value in the map.
func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.internal.Range(func(key, value interface{}) bool {
		return f(key.(K), value.(V))
	})
}

// Clear removes all items from the map
func (m *Map[K, V]) Clear() {
	m.Range(func(key K, value V) bool {
		m.Delete(key)
		return true
	})
}

// Size returns the number of items in the map
func (m *Map[K, V]) Size() int {
	count := 0
	m.Range(func(K, V) bool {
		count++
		return true
	})
	return count
}
