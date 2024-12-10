package concurrent

import (
	"sync"
)

// Set represents a thread-safe set data structure
type Set[T comparable] struct {
	sync.RWMutex
	items map[T]struct{}
}

// NewSet creates a new Set
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		items: make(map[T]struct{}),
	}
}

// Add adds an item to the set
func (s *Set[T]) Add(item T) {
	s.Lock()
	defer s.Unlock()
	s.items[item] = struct{}{}
}

// Remove removes an item from the set
func (s *Set[T]) Remove(item T) {
	s.Lock()
	defer s.Unlock()
	delete(s.items, item)
}

// Contains checks if an item exists in the set
func (s *Set[T]) Contains(item T) bool {
	s.RLock()
	defer s.RUnlock()
	_, exists := s.items[item]
	return exists
}

// Size returns the number of items in the set
func (s *Set[T]) Size() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.items)
}

// Clear removes all items from the set
func (s *Set[T]) Clear() {
	s.Lock()
	defer s.Unlock()
	s.items = make(map[T]struct{})
}

// Items returns a slice of all items in the set
func (s *Set[T]) Items() []T {
	s.RLock()
	defer s.RUnlock()
	items := make([]T, 0, len(s.items))
	for item := range s.items {
		items = append(items, item)
	}
	return items
}

func (s *Set[T]) ForEach(fn func(item T)) {
	s.RLock()
	defer s.RUnlock()
	for item := range s.items {
		fn(item)
	}
}

// Union returns a new set containing all elements from both sets
func (s *Set[T]) Union(other *Set[T]) *Set[T] {
	result := NewSet[T]()
	s.RLock()
	other.RLock()
	defer s.RUnlock()
	defer other.RUnlock()

	for item := range s.items {
		result.items[item] = struct{}{}
	}
	for item := range other.items {
		result.items[item] = struct{}{}
	}

	return result
}

// Intersection returns a new set containing elements present in both sets
func (s *Set[T]) Intersection(other *Set[T]) *Set[T] {
	result := NewSet[T]()
	s.RLock()
	other.RLock()
	defer s.RUnlock()
	defer other.RUnlock()

	for item := range s.items {
		if _, exists := other.items[item]; exists {
			result.items[item] = struct{}{}
		}
	}
	return result
}
