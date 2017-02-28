package pubsub

import (
	"context"
	"sync"
)

type topic struct {
	sync.Mutex

	name string
	done chan bool
	subs map[*subscriber]struct{}
}

type subscriber struct {
	receiver Receiver
}

type publisher struct {
	sync.Mutex

	paths map[string]*topic
}

// New creates an in-memory publisher.
func New() Publisher {
	return &publisher{
		paths: make(map[string]*topic),
	}
}

func (p *publisher) Create(c context.Context, path string) error {
	p.Lock()
	t, ok := p.paths[path]
	if !ok {
		t = &topic{
			name: path,
			done: make(chan bool),
			subs: make(map[*subscriber]struct{}),
		}
		p.paths[path] = t
	}
	p.Unlock()
	return nil
}

func (p *publisher) Publish(c context.Context, path string, message Message) error {
	p.Lock()
	t, ok := p.paths[path]
	p.Unlock()
	if !ok {
		return ErrNotFound
	}
	t.Lock()
	for sub := range t.subs {
		// todo this needs to be improved
		go sub.receiver(message)
	}
	t.Unlock()
	return nil
}

func (p *publisher) Subscribe(c context.Context, path string, receiver Receiver) error {
	p.Lock()
	t, ok := p.paths[path]
	p.Unlock()
	if !ok {
		return ErrNotFound
	}
	s := &subscriber{
		receiver: receiver,
	}
	t.Lock()
	t.subs[s] = struct{}{}
	t.Unlock()
	select {
	case <-c.Done():
	case <-t.done:
	}
	t.Lock()
	delete(t.subs, s)
	t.Unlock()
	return nil
}

func (p *publisher) Remove(c context.Context, path string) error {
	p.Lock()
	t, ok := p.paths[path]
	if ok {
		delete(p.paths, path)
	}
	close(t.done)
	p.Unlock()
	return nil
}
