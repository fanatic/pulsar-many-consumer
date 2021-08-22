package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Worker struct {
	id       int
	consumer pulsar.Consumer
}

func New(ctx context.Context, client pulsar.Client, id int, topic string) (*Worker, error) {
	// Use the client to instantiate a consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "workers",
		Type:             pulsar.Shared,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	return &Worker{id, consumer}, nil
}

func (w *Worker) Close() {
	w.consumer.Close()
}

func (w *Worker) Consumer() pulsar.Consumer {
	return w.consumer
}

func (w *Worker) Consume(ctx context.Context, baseDelayMS, maxDelayMS int, errorRate float32) {
	// infinite loop to receive messages
	go func() {
		for {
			msg, err := w.consumer.Receive(ctx)
			if err != nil {
				log.Printf("Error from receive: %d %v\n", w.id, err)
			} else {
				fmt.Printf("%d Consumed message : %v %v %v\n", w.id, msg.ProducerName(), msg.ID(), string(msg.Payload()))
			}

			time.Sleep(time.Duration(rand.Intn(maxDelayMS)+baseDelayMS) * time.Millisecond)

			if rand.Float32() < errorRate {
				log.Printf("Error on message %d %v\n", w.id, msg.ID())
				w.consumer.Nack(msg)
			}

			if w.id == 9 {
				time.Sleep(60 * time.Second)
			}

			w.consumer.Ack(msg)
		}
	}()

}
