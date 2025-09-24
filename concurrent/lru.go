package concurrent

import "sync"

type Node[K comparable, V any] struct {
	key        K
	value      V
	prev, next *Node[K, V]
}

type LRU[K comparable, V any] struct {
	capacity int
	cache    map[K]*Node[K, V]
	head     *Node[K, V]
	tail     *Node[K, V]
	mutex    sync.Mutex
	onEvicted func(K, V)
}

func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	return &LRU[K, V]{
		capacity: capacity,
		cache:    make(map[K]*Node[K, V]),
		head:     nil,
		tail:     nil,
		mutex:    sync.Mutex{},
		onEvicted: nil,
	}
}

// NewLRUWithEvict creates an LRU with an eviction callback
func NewLRUWithEvict[K comparable, V any](capacity int, onEvicted func(K, V)) *LRU[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	return &LRU[K, V]{
		capacity:  capacity,
		cache:     make(map[K]*Node[K, V]),
		head:      nil,
		tail:      nil,
		mutex:     sync.Mutex{},
		onEvicted: onEvicted,
	}
}

func (l *LRU[K, V]) Get(key K) (V, bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if node, exists := l.cache[key]; exists {
		l.moveToFront(node)
		return node.value, true
	}
	var zero V
	return zero, false
}

func (l *LRU[K, V]) Put(key K, value V) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if node, exists := l.cache[key]; exists {
		node.value = value
		l.moveToFront(node)
		return
	}

	newNode := &Node[K, V]{key: key, value: value}
	l.cache[key] = newNode

	if len(l.cache) == 1 {
		l.head = newNode
		l.tail = newNode
	} else {
		newNode.next = l.head
		l.head.prev = newNode
		l.head = newNode
	}

	if len(l.cache) > l.capacity {
		// Evict least recently used (tail)
		evicted := l.tail
		// unlink
		if l.head == l.tail {
			// single element
			l.head = nil
			l.tail = nil
		} else {
			l.tail = evicted.prev
			if l.tail != nil {
				l.tail.next = nil
			}
			evicted.prev = nil
		}
		delete(l.cache, evicted.key)
		if l.onEvicted != nil {
			l.onEvicted(evicted.key, evicted.value)
		}
	}
}

func (l *LRU[K, V]) moveToFront(node *Node[K, V]) {
	if node == l.head {
		return
	}

	if node == l.tail {
		l.tail = node.prev
		if l.tail != nil {
			l.tail.next = nil
		}
	} else {
		if node.prev != nil {
			node.prev.next = node.next
		}
		if node.next != nil {
			node.next.prev = node.prev
		}
	}

	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	}
	l.head = node
}

func (l *LRU[K, V]) Len() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return len(l.cache)
}

// Delete removes a key from the cache (no eviction callback)
func (l *LRU[K, V]) Delete(key K) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	node, exists := l.cache[key]
	if !exists {
		return
	}

	switch {
	case node == l.head && node == l.tail:
		l.head = nil
		l.tail = nil
	case node == l.head:
		l.head = node.next
		if l.head != nil {
			l.head.prev = nil
		}
	case node == l.tail:
		l.tail = node.prev
		if l.tail != nil {
			l.tail.next = nil
		}
	default:
		if node.prev != nil {
			node.prev.next = node.next
		}
		if node.next != nil {
			node.next.prev = node.prev
		}
	}
	delete(l.cache, key)
}

// Range iterates over the cache items (unordered)
func (l *LRU[K, V]) Range(f func(key K, value V) bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	for k, n := range l.cache {
		if !f(k, n.value) {
			break
		}
	}
}
