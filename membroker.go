package membroker

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	cmap "github.com/orcaman/concurrent-map"
)

const defaultPruneInterval = 5 * time.Minute

type Membroker[T any] struct {
	queue         chan *Message[T]
	messages      cmap.ConcurrentMap
	subscribers   cmap.ConcurrentMap
	pruneInterval time.Duration
	quitChan      chan bool
	exited        *uint32
}

func NewMembroker[T any](pruneInterval ...time.Duration) *Membroker[T] {
	var exited uint32 = 0

	var _pruneInterval = defaultPruneInterval
	if len(pruneInterval) > 0 {
		_pruneInterval = pruneInterval[0]
	}

	m := &Membroker[T]{
		queue:         make(chan *Message[T]),
		messages:      cmap.New(),
		subscribers:   cmap.New(),
		pruneInterval: _pruneInterval,
		quitChan:      make(chan bool, 1),
		exited:        &exited,
	}

	return m
}

// Run wraps RunContext, passing an "empty" context.Context.
func (m *Membroker[T]) Run() error {
	return m.RunContext(context.Background())
}

// RunContext starts the Membroker's broadcast and prune loops, and waits
// for the passed context to be done or Stop to be called before exiting.
func (m *Membroker[T]) RunContext(ctx context.Context) error {
	go m.broadcastLoop(ctx)
	go m.pruneLoop(ctx)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
	defer stop()

	defer m.exit()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.quitChan:
			return nil
		}
	}
}

// Stop cleanly stops the Membroker, preventing any new messages from being sent or broadcasted.
func (m *Membroker[T]) Stop() {
	if !m.Exited() {
		m.quitChan <- true
	}
}

func (m *Membroker[T]) exit() {
	if !m.Exited() {
		atomic.StoreUint32(m.exited, 1)
	}
}

// Exited returns true if the Membroker has exited for any reason.
func (m Membroker[T]) Exited() bool {
	return atomic.LoadUint32(m.exited) == 1
}

// Subscribe returns a new Subscriber which is stored in the
// Membroker's internal subscribers map.
func (m *Membroker[T]) Subscribe() *Subscriber[T] {
	s := NewSubscriber[T]()

	m.subscribers.SetIfAbsent(s.Id(), s)

	return s
}

// Unsubscribe removes the passed subscriber from the Membroker's
// internal subscribers map if present.
// If the passed subscriber is present, it is also closed.
func (m *Membroker[T]) Unsubscribe(subId string) {
	s, ok := m.subscribers.Get(subId)
	if ok {
		sub := s.(*Subscriber[T])
		sub.Close()
	}

	return
}

// Send sends a Message to all subscribers.
func (m *Membroker[T]) Send(msg *Message[T]) {
	m.messages.SetIfAbsent(msg.Id(), msg)
	go func(m *Membroker[T], msg *Message[T]) {
		m.queue <- msg
	}(m, msg)
}

func (m Membroker[T]) Message(id string) *Message[T] {
	rawMsg, ok := m.messages.Get(id)
	if ok {
		if msg, castOk := rawMsg.(*Message[T]); castOk {
			return msg
		} else {
			panic("unable to cast interface{} to *Message[T]")
		}
	}

	return nil
}

func (m Membroker[T]) broadcastLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-m.queue:
			for _, s := range m.subscribers.Items() {
				sub := s.(*Subscriber[T])
				go m.broadcast(msg, sub)
			}
		}
	}
}

func (m Membroker[T]) broadcast(msg *Message[T], subscriber *Subscriber[T]) {
	if !subscriber.Closed() {
		subscriber.Send(msg)
	}
}

func (m *Membroker[T]) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(m.pruneInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pruneMessages()
			m.pruneSubscribers()
		}
	}
}

func (m *Membroker[T]) pruneMessages() {
	msgMap := m.messages

	for id, rawMsg := range msgMap.Items() {
		msg := rawMsg.(*Message[T])
		if msg.CanExpire() && msg.Expired() {
			m.messages.Remove(id)
		}
	}

	return
}

func (m *Membroker[T]) pruneSubscribers() {
	subMap := m.subscribers

	for id, rawSub := range subMap.Items() {
		sub := rawSub.(*Subscriber[T])
		if sub.Closed() {
			m.subscribers.Remove(id)
		}
	}

	return
}
