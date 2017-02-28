package gcp

import (
	"context"
	"io/ioutil"
	"log"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/cncd/pubsub"
	"github.com/cncd/pubsub/gcp/internal"
)

type conn struct {
	base   pubsub.Publisher
	opts   *Options
	client *internal.Client
}

// New returns a pubsub messaging system backed by Google Cloud pubusb.
func New(opts ...Option) (pubsub.Publisher, error) {
	conn := new(conn)
	conn.base = pubsub.New()
	conn.opts = new(Options)
	conn.opts.topic = "pubsub"
	conn.opts.subscription = "default"
	for _, opt := range opts {
		opt(conn.opts)
	}

	jsonToken, err := ioutil.ReadFile(conn.opts.tokenpath)
	if err != nil {
		return nil, err
	}
	jwt, err := google.JWTConfigFromJSON(jsonToken, "https://www.googleapis.com/auth/pubsub")
	if err != nil {
		return nil, err
	}
	src := jwt.TokenSource(oauth2.NoContext)
	cli := oauth2.NewClient(oauth2.NoContext, src)
	client := internal.NewClient(cli)

	conn.client = client
	go conn.poll()
	return conn, nil
}

func (c *conn) Create(ctx context.Context, id string) error {
	return c.base.Create(ctx, id)
}

func (c *conn) Publish(ctx context.Context, id string, message pubsub.Message) error {
	_, err := c.client.Publish(ctx, c.opts.project, c.opts.topic, fromMessage(message, id))
	return err
}

func (c *conn) Subscribe(ctx context.Context, id string, receiver pubsub.Receiver) error {
	return c.base.Subscribe(ctx, id, receiver)
}

func (c *conn) Remove(ctx context.Context, id string) error {
	return c.base.Remove(ctx, id)
}

func (c *conn) poll() {
	for {
		log.Println("pubsub: pull: polling for messages")

		messages, err := c.client.Pull(
			context.Background(),
			c.opts.project,
			c.opts.subscription,
			100,
		)
		if err != nil {
			log.Printf("pubsub: pull: error: %s", err)
			continue
		}

		var ackIDs []string
		for _, message := range messages {
			ackIDs = append(ackIDs, message.AckID)
		}

		err = c.client.Ack(
			context.Background(),
			c.opts.project,
			c.opts.subscription,
			ackIDs...,
		)
		if err != nil {
			log.Printf("pubsub: ack: error: %s", err)
		}

		for _, message := range messages {
			msg, path := fromGoogleMessage(message)
			c.base.Publish(context.Background(), path, msg)
		}
	}
}

// helper function converts the common pubsub message format
// to the Google Pubsub message format.
func fromMessage(from pubsub.Message, path string) internal.Message {
	labels := copymap(from.Labels)
	labels["__path__"] = path

	var to internal.Message
	to.Data = from.Data
	to.Attributes = labels
	return to
}

// helper function that converts a Google Pubsub message to
// the common message format.
func fromGoogleMessage(from internal.Message) (pubsub.Message, string) {
	attributes := copymap(from.Attributes)
	delete(attributes, "__path__")

	to := pubsub.Message{}
	to.Data = from.Data
	to.Labels = attributes
	return to, from.Attributes["__path__"]
}

// helper function creates a copy of the map
func copymap(from map[string]string) map[string]string {
	to := make(map[string]string)
	for k, v := range from {
		to[k] = v
	}
	return to
}

//

//
// type conn struct {
// 	base   pubsub.Pubsub
// 	opts   *Options
// 	client *internal.Client
// }
//
// // New returns a pubsub messaging system backed by Google Cloud pubusb.
// func New(opts ...Option) (pubsub.Pubsub, error) {
// 	conn := new(conn)
// 	conn.base = pubsub.New()
// 	conn.opts = new(Options)
// 	conn.opts.topic = "pubsub"
// 	conn.opts.subscription = "default"
// 	for _, opt := range opts {
// 		opt(conn.opts)
// 	}
//
// 	client, err := gpubsub.NewClient(context.Background(), conn.opts.project,
// 		option.WithServiceAccountFile(conn.opts.tokenpath),
// 	)
// 	if err != nil {
// 		return nil, err
// 	}
// 	conn.topic = client.Topic(conn.opts.topic)
//
// 	conn.client = client
// 	go conn.poll()
// 	return conn, nil
// }
//
// func (c *conn) Create(ctx context.Context, id string) error {
// 	return c.base.Create(ctx, id)
// }
//
// func (c *conn) Publish(ctx context.Context, id string, message pubsub.Message) error {
// 	_, err := c.topic.Publish(ctx, fromMessage(message, id))
// 	return err
// }
//
// func (c *conn) Subscribe(ctx context.Context, id string, receiver pubsub.Receiver) error {
// 	return c.base.Subscribe(ctx, id, receiver)
// }
//
// func (c *conn) Remove(ctx context.Context, id string) error {
// 	return c.base.Remove(ctx, id)
// }
//
// func (c *conn) poll() {
// 	sub := c.client.Subscription(c.opts.subscription)
//
// 	it, err := sub.Pull(context.Background(),
// 		gpubsub.MaxPrefetch(100),
// 		gpubsub.MaxExtension(time.Minute),
// 	)
// 	if err != nil {
// 		fmt.Println(err)
// 		return
// 	}
// 	defer it.Stop()
//
// 	for {
// 		m, err := it.Next()
// 		if err != nil {
// 			fmt.Println(err)
// 			break
// 		}
// 		m.Done(true)
//
// 		message, path := fromGoogleMessage(m)
// 		c.base.Publish(context.Background(), path, message)
// 	}
// }
//
// // helper function converts the common pubsub message format
// // to the Google Pubsub message format.
// func fromMessage(from pubsub.Message, path string) *gpubsub.Message {
// 	labels := copymap(from.Labels)
// 	labels["__path__"] = path
//
// 	to := new(gpubsub.Message)
// 	to.Data = from.Data
// 	to.Attributes = labels
// 	return to
// }
//
// // helper function that converts a Google Pubsub message to
// // the common message format.
// func fromGoogleMessage(from *gpubsub.Message) (pubsub.Message, string) {
// 	attributes := copymap(from.Attributes)
// 	delete(attributes, "__path__")
//
// 	to := pubsub.Message{}
// 	to.Data = from.Data
// 	to.Labels = attributes
// 	return to, from.Attributes["__path__"]
// }
//
// // helper function creates a copy of the map
// func copymap(from map[string]string) map[string]string {
// 	to := make(map[string]string)
// 	for k, v := range from {
// 		to[k] = v
// 	}
// 	return to
// }
