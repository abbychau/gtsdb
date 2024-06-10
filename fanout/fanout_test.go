package fanout

import (
	models "gtsdb/models"
	"sync"
	"testing"
	"time"
)

func TestFanout(t *testing.T) {
	fanout := NewFanout()
	fanout.Start()

	var wg sync.WaitGroup
	wg.Add(2)

	consumer1 := &Consumer{
		ID: 1,
		Callback: func(dataPoint models.DataPoint) {
			if dataPoint.Timestamp != 1 || dataPoint.Value != 1 {
				t.Error("Unexpected data point in consumer1:", dataPoint)
			}
			t.Log("Consumer1 received data point:", dataPoint)
			wg.Done()
		},
	}

	consumer2 := &Consumer{
		ID: 2,
		Callback: func(dataPoint models.DataPoint) {
			if dataPoint.Timestamp != 1 || dataPoint.Value != 1 {
				t.Error("Unexpected data point in consumer2:", dataPoint)
			}
			t.Log("Consumer2 received data point:", dataPoint)
			wg.Done()
		},
	}

	fanout.AddConsumer(consumer1.ID, consumer1.Callback)
	fanout.AddConsumer(consumer2.ID, consumer2.Callback)

	fanout.Publish(models.DataPoint{Timestamp: 1, Value: 1})

	// wg.Wait() with timeout, if it takes too long, prompt t.Error
	if waitTimeout(&wg, 1*time.Second) {
		t.Error("Timed out waiting for consumers to receive data points")
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
