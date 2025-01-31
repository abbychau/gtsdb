package fanout

import (
	"sync/atomic"
	"unsafe"

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
	pending   unsafe.Pointer // *models.DataPoint
}

func NewFanout(_ int) *Fanout {
	f := &Fanout{
		consumers: *concurrent.NewSet[*Consumer](),
	}
	// Initialize with empty DataPoint
	initial := &models.DataPoint{}
	atomic.StorePointer(&f.pending, unsafe.Pointer(initial))
	return f
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
	// Allocate new DataPoint on heap
	newMsg := &msg
	// Atomically store the new message
	atomic.StorePointer(&f.pending, unsafe.Pointer(newMsg))

	// Get latest message
	ptr := atomic.LoadPointer(&f.pending)
	message := (*models.DataPoint)(ptr)

	// Process synchronously for all consumers
	for _, c := range f.GetConsumers() {
		c.Callback(*message)
	}
}
