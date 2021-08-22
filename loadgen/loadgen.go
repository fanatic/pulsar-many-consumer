package loadgen

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Generator struct {
	producer pulsar.Producer
}

func New(ctx context.Context, client pulsar.Client, topic string) (*Generator, error) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Generator{producer}, nil
}

func (g *Generator) Close() {
	g.producer.Close()
}

func (g *Generator) GenerateLoad(ctx context.Context, msgPerSecond int) {
	asyncMsg := pulsar.ProducerMessage{
		Key:     fmt.Sprintf("%d", rand.Intn(10000)),
		Payload: []byte{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'},
		Properties: map[string]string{
			"reply-to": "loadgen-results",
		},
	}

	ticker := time.NewTicker(time.Duration((1000 * time.Millisecond) / time.Duration(msgPerSecond)))
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				g.producer.SendAsync(ctx, &asyncMsg, func(msgID pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
					if err != nil {
						log.Printf("Failed to send message: %s", err)
					}
					log.Printf("published with the message ID %v\n", msgID)
				})
			}
		}
	}()
}
