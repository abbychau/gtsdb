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
	messageCh chan models.DataPoint
}

func NewFanout() *Fanout {
	fanoutManager := &Fanout{
		consumers: *concurrent.NewSet[*Consumer](),
		messageCh: make(chan models.DataPoint),
	}
	go fanoutManager.producer()
	return fanoutManager
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
	f.messageCh <- msg
}

func (f *Fanout) producer() {
	for msg := range f.messageCh {

		for _, c := range f.GetConsumers() {
			go c.Callback(msg)
		}
	}
}
