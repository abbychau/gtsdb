package fanout

import (
	models "gtsdb/models"
	"sync"
	"testing"
	"time"
)

func TestBasicFanout(t *testing.T) {
	fanout := NewFanout()
	fanout.Start()

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
	fanout := NewFanout()
	fanout.Start()

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
	fanout := NewFanout()
	fanout.Start()

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
