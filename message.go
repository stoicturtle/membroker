package membroker

import (
	"sync"
	"time"

	"github.com/rs/xid"
)

type Message[T any] struct {
	id           xid.ID
	data         T
	requiredAcks uint64
	acks         *sync.Map
	expiration   *time.Time
}

func NewMessage[T any](data T, requiredAcks uint64) *Message[T] {
	return &Message[T]{
		id:           xid.New(),
		data:         data,
		requiredAcks: requiredAcks,
		acks:         new(sync.Map),
	}
}

func (m Message[T]) Id() string {
	return m.id.String()
}

func (m Message[T]) Data() T {
	return m.data
}

func (m Message[T]) RequiredAcks() uint64 {
	return m.requiredAcks
}

func (m Message[T]) Acks() uint64 {
	var numAcks uint64

	m.acks.Range(func(any, any) bool {
		numAcks++
		return true
	})

	return numAcks
}

func (m Message[T]) AckedBy(subscriberId string) bool {
	_, ok := m.acks.Load(subscriberId)
	return ok
}

func (m Message[T]) Expiration() time.Time {
	var t time.Time

	if m.expiration != nil {
		t = *m.expiration
	}

	return t
}

func (m *Message[T]) SetExpiration(exp time.Time) *Message[T] {
	m.expiration = &exp
	return m
}

func (m Message[T]) CanExpire() bool {
	return m.expiration != nil
}

func (m Message[T]) Expired() bool {
	if m.CanExpire() {
		return time.Now().After(m.Expiration())
	}

	return false
}

func (m Message[T]) HasSufficientAcks() bool {
	return m.Acks() >= m.requiredAcks
}

func (m *Message[T]) Ack(subscriberId string) {
	m.acks.Store(subscriberId, true)
}
