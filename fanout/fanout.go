package fanout

import (
	models "gtsdb/models"
	"sync"
)

type Consumer struct {
	ID       int
	Callback func(models.DataPoint)
}

type Fanout struct {
	consumers []*Consumer
	messageCh chan models.DataPoint
	mu        sync.RWMutex
	wg        sync.WaitGroup
}

func NewFanout() *Fanout {
	return &Fanout{
		consumers: make([]*Consumer, 0),
		messageCh: make(chan models.DataPoint),
	}
}

func (f *Fanout) Start() {
	go f.producer()
}

func (f *Fanout) AddConsumer(id int, callback func(models.DataPoint)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	consumer := &Consumer{ID: id, Callback: callback}
	f.consumers = append(f.consumers, consumer)
	f.wg.Add(1)
}

func (f *Fanout) GetConsumers() []*Consumer {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.consumers
}

func (f *Fanout) RemoveConsumer(id int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, c := range f.consumers {
		if c.ID == id {
			f.consumers = append(f.consumers[:i], f.consumers[i+1:]...)
			f.wg.Done()
			break
		}
	}
}

func (f *Fanout) Publish(msg models.DataPoint) {
	f.messageCh <- msg
}

func (f *Fanout) Wait() {
	f.wg.Wait()
}

func (f *Fanout) producer() {
	for msg := range f.messageCh {
		f.mu.RLock()
		for _, c := range f.consumers {
			go c.Callback(msg)
		}
		f.mu.RUnlock()
	}
}
