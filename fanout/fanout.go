package fanout

import (
	"fmt"
	"sync"

	models "gtsdb/models"
)

type Consumer struct {
	ID       int
	Callback func(models.DataPoint)
}

type Fanout struct {
	consumers        []*Consumer
	consumerCh       chan *Consumer
	removeConsumerCh chan int
	messageCh        chan models.DataPoint
	wg               sync.WaitGroup
}

func NewFanout() *Fanout {
	return &Fanout{
		consumers:        make([]*Consumer, 0),
		consumerCh:       make(chan *Consumer),
		removeConsumerCh: make(chan int),
		messageCh:        make(chan models.DataPoint),
	}
}

func (f *Fanout) Start() {
	go f.producer()
	go f.consumerManager()
}

func (f *Fanout) AddConsumer(id int, callback func(models.DataPoint)) {
	consumer := &Consumer{ID: id, Callback: callback}
	f.consumerCh <- consumer
}

func (f *Fanout) RemoveConsumer(id int) {
	f.removeConsumerCh <- id
}

func (f *Fanout) Publish(msg models.DataPoint) {
	f.messageCh <- msg
}

func (f *Fanout) Wait() {
	f.wg.Wait()
}

func (f *Fanout) producer() {
	for msg := range f.messageCh {
		for _, c := range f.consumers {
			go c.Callback(msg)
		}
	}
}

func (f *Fanout) consumerManager() {
	for {
		select {
		case consumer := <-f.consumerCh:
			f.consumers = append(f.consumers, consumer)
			fmt.Printf("Added consumer %d\n", consumer.ID)
			f.wg.Add(1)
		case id := <-f.removeConsumerCh:
			for i, c := range f.consumers {
				if c.ID == id {
					f.consumers = append(f.consumers[:i], f.consumers[i+1:]...)
					fmt.Printf("Removed consumer %d\n", id)
					f.wg.Done()
					break
				}
			}
		}
	}
}
