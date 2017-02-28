package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPubsub(t *testing.T) {
	var (
		wg sync.WaitGroup

		testTopic   = "test"
		testMessage = Message{
			Data: []byte("test"),
		}
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	broker := New()
	broker.Create(ctx, testTopic)
	go func() {
		broker.Subscribe(ctx, testTopic, func(message Message) { wg.Done() })
	}()
	go func() {
		broker.Subscribe(ctx, testTopic, func(message Message) { wg.Done() })
	}()

	<-time.After(time.Millisecond)

	wg.Add(2)
	go func() {
		broker.Publish(ctx, testTopic, testMessage)
	}()

	wg.Wait()
	cancel()
}
