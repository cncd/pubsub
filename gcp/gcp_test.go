package gcp

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cncd/pubsub"
)

var (
	testProject      = os.Getenv("PUBSUB_TEST_GOOGLE_PROJECT")
	testTopic        = os.Getenv("PUBSUB_TEST_GOOGLE_TOPIC")
	testSubscription = os.Getenv("PUBSUB_TEST_GOOGLE_SUBSCRIPTION")
	testTokenPath    = os.Getenv("PUBSUB_TEST_GOOGLE_JSON_TOKEN_PATH")
)

func TestGooglePubsub(t *testing.T) {
	switch {
	case testProject == "":
		t.Skipf("skip google pubsub tests, missing PUBSUB_TEST_GOOGLE_PROJECT")
	case testTopic == "":
		t.Skipf("skip google pubsub tests, missing PUBSUB_TEST_GOOGLE_TOPIC")
	case testSubscription == "":
		t.Skipf("skip google pubsub tests, missing PUBSUB_TEST_GOOGLE_SUBSCRIPTION")
	case testTokenPath == "":
		t.Skipf("skip google pubsub tests, missing PUBSUB_TEST_GOOGLE_JSON_TOKEN_PATH")
	}

	var (
		wg sync.WaitGroup

		testDest    = "test"
		testMessage = pubsub.Message{
			Data: []byte("test"),
		}
	)

	client, err := New(
		WithProject(testProject),
		WithSubscription(testSubscription),
		WithTopic(testTopic),
		WithServiceAccountToken(testTokenPath),
	)
	if err != nil {
		t.Errorf("error creating google pubsub client. %s", err)
		return
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	client.Create(ctx, testDest)
	go func() {
		client.Subscribe(ctx, testDest, func(message pubsub.Message) { wg.Done() })
	}()
	go func() {
		client.Subscribe(ctx, testDest, func(message pubsub.Message) { wg.Done() })
	}()

	<-time.After(time.Millisecond)

	wg.Add(2)
	go func() {
		client.Publish(ctx, testDest, testMessage)
	}()

	wg.Wait()
	cancel()
}
