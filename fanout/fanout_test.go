package fanout

import (
	models "gtsdb/models"
	"sync"
	"testing"
	"time"
)

func TestBasicFanout(t *testing.T) {
	fanout := NewFanout(2)

	var wg sync.WaitGroup
	wg.Add(2)

	received := make(map[int]models.DataPoint)
	mu := sync.Mutex{}

	for i := 1; i <= 2; i++ {
		id := i
		fanout.AddConsumer(id, func(dp models.DataPoint) {
			mu.Lock()
			received[id] = dp
			mu.Unlock()
			wg.Done()
		})
	}

	testPoint := models.DataPoint{Timestamp: 1, Value: 1}
	fanout.Publish(testPoint)

	if waitTimeout(&wg, time.Second) {
		t.Fatal("Timeout waiting for consumers")
	}

	mu.Lock()
	for id, dp := range received {
		if dp != testPoint {
			t.Errorf("Consumer %d received incorrect data point. Got %v, want %v", id, dp, testPoint)
		}
	}
	mu.Unlock()
}

func TestConsumerRemoval(t *testing.T) {
	fanout := NewFanout(2)

	var callCount int
	var mu sync.Mutex

	id := 1
	fanout.AddConsumer(id, func(dp models.DataPoint) {
		mu.Lock()
		callCount++
		mu.Unlock()
	})

	fanout.Publish(models.DataPoint{Timestamp: 1, Value: 1})
	time.Sleep(100 * time.Millisecond)

	fanout.RemoveConsumer(id)
	fanout.Publish(models.DataPoint{Timestamp: 2, Value: 2})
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if callCount != 1 {
		t.Errorf("Expected exactly 1 call, got %d", callCount)
	}
	mu.Unlock()
}

func TestConcurrentPublish(t *testing.T) {
	fanout := NewFanout(10) // Larger buffer for concurrent test

	messageCount := 100
	consumerCount := 3
	var wg sync.WaitGroup
	wg.Add(messageCount * consumerCount)

	received := make(map[int]int)
	mu := sync.Mutex{}

	for i := 1; i <= consumerCount; i++ {
		id := i
		fanout.AddConsumer(id, func(dp models.DataPoint) {
			mu.Lock()
			received[id]++
			mu.Unlock()
			wg.Done()
		})
	}

	for i := 0; i < messageCount; i++ {
		go fanout.Publish(models.DataPoint{Timestamp: int64(i), Value: float64(i)})
	}

	if waitTimeout(&wg, 2*time.Second) {
		t.Fatal("Timeout waiting for concurrent publishes")
	}

	mu.Lock()
	for id, count := range received {
		if count != messageCount {
			t.Errorf("Consumer %d received %d messages, expected %d", id, count, messageCount)
		}
	}
	mu.Unlock()
}

func TestGetConsumers(t *testing.T) {
	fanout := NewFanout(2)

	// Add three consumers
	consumers := []int{1, 2, 3}
	for _, id := range consumers {
		fanout.AddConsumer(id, func(dp models.DataPoint) {})
	}

	// Get consumers and verify
	actualConsumers := fanout.GetConsumers()
	if len(actualConsumers) != len(consumers) {
		t.Errorf("Expected %d consumers, got %d", len(consumers), len(actualConsumers))
	}

	// Verify consumer IDs
	consumerMap := make(map[int]bool)
	for _, c := range actualConsumers {
		consumerMap[c.ID] = true
	}

	for _, id := range consumers {
		if !consumerMap[id] {
			t.Errorf("Consumer with ID %d not found", id)
		}
	}

	// Remove a consumer and verify again
	fanout.RemoveConsumer(2)
	actualConsumers = fanout.GetConsumers()
	if len(actualConsumers) != len(consumers)-1 {
		t.Errorf("Expected %d consumers after removal, got %d", len(consumers)-1, len(actualConsumers))
	}
}

func TestNewFanoutInvalidBuffer(t *testing.T) {
	fanout := NewFanout(0)
	if cap(fanout.pending) != 1 {
		t.Errorf("Expected buffer size 1 for invalid input, got %d", cap(fanout.pending))
	}

	fanout = NewFanout(-1)
	if cap(fanout.pending) != 1 {
		t.Errorf("Expected buffer size 1 for invalid input, got %d", cap(fanout.pending))
	}
}

func TestGetConsumer(t *testing.T) {
	fanout := NewFanout(1)

	// Test getting non-existent consumer
	if consumer := fanout.GetConsumer(1); consumer != nil {
		t.Error("Expected nil for non-existent consumer")
	}

	// Add a consumer and verify we can get it
	testCallback := func(dp models.DataPoint) {}
	fanout.AddConsumer(1, testCallback)

	consumer := fanout.GetConsumer(1)
	if consumer == nil {
		t.Error("Expected to find consumer with ID 1")
	}
	if consumer.ID != 1 {
		t.Errorf("Expected consumer ID 1, got %d", consumer.ID)
	}

	// Add another consumer and verify we can still get the correct one
	fanout.AddConsumer(2, testCallback)
	consumer = fanout.GetConsumer(1)
	if consumer == nil || consumer.ID != 1 {
		t.Error("Failed to get correct consumer after adding another")
	}

	// Remove the consumer and verify it's gone
	fanout.RemoveConsumer(1)
	if consumer := fanout.GetConsumer(1); consumer != nil {
		t.Error("Consumer should have been removed")
	}
}

func TestConcurrentConsumerModification(t *testing.T) {
	fanout := NewFanout(1)

	// Add initial consumers
	for i := 1; i <= 5; i++ {
		fanout.AddConsumer(i, func(dp models.DataPoint) {})
	}

	var wg sync.WaitGroup
	operations := 100
	wg.Add(operations * 2) // Add and remove operations

	// Concurrently add and remove consumers
	for i := 6; i < 6+operations; i++ {
		id := i
		go func() {
			defer wg.Done()
			fanout.AddConsumer(id, func(dp models.DataPoint) {})
		}()

		go func() {
			defer wg.Done()
			fanout.RemoveConsumer(id - 5) // Remove previously added consumers
		}()
	}

	if waitTimeout(&wg, 2*time.Second) {
		t.Fatal("Timeout waiting for concurrent operations")
	}

	// Verify the final state
	consumers := fanout.GetConsumers()
	if len(consumers) == 0 {
		t.Error("Expected some consumers to remain")
	}

	// Ensure no duplicate IDs
	seen := make(map[int]bool)
	for _, c := range consumers {
		if seen[c.ID] {
			t.Errorf("Found duplicate consumer ID: %d", c.ID)
		}
		seen[c.ID] = true
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
