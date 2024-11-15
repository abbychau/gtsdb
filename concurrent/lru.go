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
}

func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	return &LRU[K, V]{
		capacity: capacity,
		cache:    make(map[K]*Node[K, V]),
		head:     nil,
		tail:     nil,
		mutex:    sync.Mutex{},
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
		delete(l.cache, l.tail.key)
		l.tail = l.tail.prev
		l.tail.next = nil
	}
}

func (l *LRU[K, V]) moveToFront(node *Node[K, V]) {
	if node == l.head {
		return
	}

	if node == l.tail {
		l.tail = node.prev
		l.tail.next = nil
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}

	node.prev = nil
	node.next = l.head
	l.head.prev = node
	l.head = node
}

func (l *LRU[K, V]) Len() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return len(l.cache)
}
