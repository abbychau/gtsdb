package concurrent

//🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹🐹
import "sync"

// HashMap represents a thread-safe generic map structure
type HashMap[K comparable, V any] struct {
	sync.RWMutex
	items map[K]V
}

// NewHashMap creates and initializes a new HashMap
func NewHashMap[K comparable, V any]() *HashMap[K, V] {
	return &HashMap[K, V]{
		items: make(map[K]V),
	}
}

// Put adds or updates an element in the map
func (h *HashMap[K, V]) Put(key K, value V) {
	h.Lock()
	defer h.Unlock()
	h.items[key] = value
}

// Set (alias of Put)
func (h *HashMap[K, V]) Set(key K, value V) {
	h.Put(key, value)
}

// Get retrieves an element from the map
func (h *HashMap[K, V]) Get(key K) (V, bool) {
	h.RLock()
	defer h.RUnlock()
	value, exists := h.items[key]
	return value, exists
}

// Delete removes an element from the map
func (h *HashMap[K, V]) Delete(key K) {
	h.Lock()
	defer h.Unlock()
	delete(h.items, key)
}

// Contains checks if a key exists in the map
func (h *HashMap[K, V]) Contains(key K) bool {
	h.RLock()
	defer h.RUnlock()
	_, exists := h.items[key]
	return exists
}

// Size returns the number of elements in the map
func (h *HashMap[K, V]) Size() int {
	h.RLock()
	defer h.RUnlock()
	return len(h.items)
}

// Clear removes all elements from the map
func (h *HashMap[K, V]) Clear() {
	h.Lock()
	defer h.Unlock()
	h.items = make(map[K]V)
}

// ForEach executes a function for each key-value pair in the map
func (h *HashMap[K, V]) ForEach(fn func(key K, value V)) {
	h.RLock()
	defer h.RUnlock()
	for k, v := range h.items {
		fn(k, v)
	}
}

// Values
func (h *HashMap[K, V]) Values() []V {
	h.RLock()
	defer h.RUnlock()
	values := make([]V, 0, len(h.items))
	for _, v := range h.items {
		values = append(values, v)
	}
	return values
}
