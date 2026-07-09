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
}

func NewFanout(_ int) *Fanout {
	return &Fanout{
		consumers: *concurrent.NewSet[*Consumer](),
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
	for _, c := range f.GetConsumers() {
		if c.ID == id {
			f.consumers.Remove(c)
			utils.Log("Removed consumer %d", id)
			return
		}
	}
}

func (f *Fanout) Publish(msg models.DataPoint) {
	// Process synchronously for all consumers
	for _, c := range f.GetConsumers() {
		c.Callback(msg)
	}
}
