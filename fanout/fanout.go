package fanout

import (
	"gtsdb/concurrent"
	models "gtsdb/models"
	"gtsdb/utils"
)

type Consumer struct {
	ID       int
	Callback func(models.DataPoint)
}

type Fanout struct {
	consumers concurrent.Set[*Consumer]
	pending   chan models.DataPoint
}

func NewFanout(bufferSize int) *Fanout {
	if bufferSize < 1 {
		bufferSize = 1
	}
	return &Fanout{
		consumers: *concurrent.NewSet[*Consumer](),
		pending:   make(chan models.DataPoint, bufferSize),
	}
}

func (f *Fanout) AddConsumer(id int, callback func(models.DataPoint)) {
	consumer := &Consumer{ID: id, Callback: callback}
	f.consumers.Add(consumer)
}

func (f *Fanout) GetConsumers() []*Consumer {
	return f.consumers.Items()
}

func (f *Fanout) GetConsumer(id int) *Consumer {
	for _, c := range f.GetConsumers() {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func (f *Fanout) RemoveConsumer(id int) {
	f.consumers.Remove(f.GetConsumer(id))
	utils.Log("Removed consumer %d", id)
}

func (f *Fanout) Publish(msg models.DataPoint) {
	// First try to queue the message
	f.pending <- msg

	// Get message once
	message := <-f.pending

	// Then process it synchronously for all consumers
	for _, c := range f.GetConsumers() {
		c.Callback(message)
	}
}
