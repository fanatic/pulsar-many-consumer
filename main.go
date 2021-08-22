package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fanatic/pulsar-many-consumer/loadgen"
	"github.com/fanatic/pulsar-many-consumer/worker"
)

const (
	Topic                      = "topic"
	WorkerCount                = 9
	WorkerBaseDelayMS          = 1000
	WorkerMaxDelayMS           = 100
	WorkerErrorRate            = 0.01
	GeneratorMessagesPerSecond = 1.0
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	ctx := context.Background()

	// Setup workers, each sleeping for random delay to simulate work
	// We introduce a random error rate of 1% to simulate some failure
	for i := 0; i < WorkerCount; i++ {
		w, err := worker.New(ctx, client, i, Topic)
		if err != nil {
			log.Fatalf("Could not instantiate worker: %v", err)
		}
		w.Consume(ctx, WorkerBaseDelayMS, WorkerMaxDelayMS, WorkerErrorRate)
	}

	// Setup producer loop to generate load
	g, err := loadgen.New(ctx, client, Topic)
	if err != nil {
		log.Fatalf("Could not instantiate generator: %v", err)
	}
	g.GenerateLoad(ctx, GeneratorMessagesPerSecond)

	//TODO: Track task execution time (queue+execution) and report

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
