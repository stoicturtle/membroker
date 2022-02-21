package membroker

import (
	"sync/atomic"

	"github.com/rs/xid"
)

type Subscriber[T any] struct {
	id     xid.ID
	sink   chan *Message[T]
	closed *uint32
}

func NewSubscriber[T any]() *Subscriber[T] {
	var closed uint32 = 0

	return &Subscriber[T]{
		id:     xid.New(),
		sink:   make(chan *Message[T]),
		closed: &closed,
	}
}

func (s Subscriber[T]) Id() string {
	return s.id.String()
}

func (s Subscriber[T]) Recv() *Message[T] {
	return <-s.sink
}

func (s Subscriber[T]) RecvAll() <-chan *Message[T] {
	return s.sink
}

func (s *Subscriber[T]) Send(msg *Message[T]) {
	s.sink <- msg
}

func (s *Subscriber[T]) Close() {
	if !s.Closed() {
		close(s.sink)
		atomic.StoreUint32(s.closed, 1)
	}
}

func (s Subscriber[T]) Closed() bool {
	return atomic.LoadUint32(s.closed) == 1
}
