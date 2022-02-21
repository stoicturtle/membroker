package membroker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stoicturtle/membroker"
)

const defaultPrune = 2 * time.Minute

func startMembroker(t *testing.T, prune time.Duration) (*membroker.Membroker[string], context.Context, context.CancelFunc, <-chan error) {
	t.Helper()

	m := membroker.NewMembroker[string](prune)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)

	ch := make(chan error)

	go func(ctx context.Context, m *membroker.Membroker[string], ch chan<- error) {
		ch <- m.RunContext(ctx)
	}(ctx, m, ch)

	return m, ctx, cancel, ch
}

func newSubscriber(t *testing.T, m *membroker.Membroker[string]) (*membroker.Subscriber[string], bool) {
	sub := m.Subscribe()

	return sub, assertValidSubscriber(t, sub)
}

const msgData string = "Hello, world!"

func makeMessage(t *testing.T, acks uint64) *membroker.Message[string] {
	t.Helper()

	return membroker.NewMessage[string](msgData, acks)
}

func startSubscribers(
	t *testing.T,
	ctx context.Context,
	m *membroker.Membroker[string],
	n int,
	breakAfter, ackAfter *int,
) (*membroker.Subscriber[string], *sync.WaitGroup) {

	var (
		singleSub *membroker.Subscriber[string]
		wg        sync.WaitGroup
	)

	for i := 0; i < n; i++ {
		sub, validSub := newSubscriber(t, m)
		if !validSub {
			t.FailNow()
		}

		if i == 0 {
			singleSub = sub
		}

		wg.Add(1)
		switch {
		case breakAfter != nil:
			go ackBreakAfter(ctx, sub, &wg, *breakAfter)
		case ackAfter != nil:
			go ackAfterReceives(ctx, sub, &wg, *ackAfter)
		default:
			go ackMsg(ctx, sub, &wg)
		}
	}

	return singleSub, &wg
}

func TestMembroker_Send(t *testing.T) {
	m, _, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	sub, validSub := newSubscriber(t, m)
	if !validSub {
		return
	}

	msg := makeMessage(t, 0)
	m.Send(msg)

	recv := sub.Recv()
	assert.NotNil(t, recv)

	cancel()

	assert.Equal(t, msg.Id(), recv.Id())
	assert.Equal(t, msg.Data(), recv.Data())

	assert.False(t, recv.Expired())
	assert.False(t, recv.CanExpire())
	assert.True(t, recv.HasSufficientAcks())

	assertContextCancelledError(t, <-ch)
}

func TestMembroker_SufficientAcks(t *testing.T) {
	m, ctx, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	_, wg := startSubscribers(t, ctx, m, 5, nil, nil)

	msg := makeMessage(t, 4)
	m.Send(msg)

	wg.Wait()

	cancel()

	msg = m.Message(msg.Id())
	assert.Greater(t, msg.Acks(), msg.RequiredAcks())

	assertContextCancelledError(t, <-ch)
}

func TestMembroker_InsufficientAcks(t *testing.T) {
	m, ctx, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	_, wg := startSubscribers(t, ctx, m, 5, nil, nil)

	msg := makeMessage(t, 8)
	m.Send(msg)

	wg.Wait()

	cancel()

	msg = m.Message(msg.Id())
	assert.Less(t, msg.Acks(), msg.RequiredAcks())

	assertContextCancelledError(t, <-ch)
}

func TestMembroker_AckedBy(t *testing.T) {
	m, ctx, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	singleSub, wg := startSubscribers(t, ctx, m, 5, nil, nil)

	assert.NotNil(t, singleSub)

	msg := makeMessage(t, 5)
	msg2 := makeMessage(t, 2)
	m.Send(msg)

	wg.Wait()

	m.Send(msg2)

	cancel()

	msg = m.Message(msg.Id())
	assert.GreaterOrEqual(t, msg.Acks(), msg.RequiredAcks())
	assert.True(t, msg.AckedBy(singleSub.Id()))

	assert.False(t, msg2.HasSufficientAcks())

	assertContextCancelledError(t, <-ch)
}

func TestMembroker_DuplicateAckedBy(t *testing.T) {
	m, ctx, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	breakAfter := 2
	singleSub, wg := startSubscribers(t, ctx, m, 5, &breakAfter, nil)

	assert.NotNil(t, singleSub)

	msg := makeMessage(t, 5)
	m.Send(msg)
	m.Send(msg)

	wg.Wait()

	cancel()

	msg = m.Message(msg.Id())
	assert.GreaterOrEqual(t, msg.Acks(), msg.RequiredAcks())
	assert.True(t, msg.AckedBy(singleSub.Id()))

	assertContextCancelledError(t, <-ch)
}

func TestMembroker_Rebroadcast(t *testing.T) {
	m, ctx, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	ackAfter := 1
	singleSub, wg := startSubscribers(t, ctx, m, 5, nil, &ackAfter)

	assert.NotNil(t, singleSub)

	msg := makeMessage(t, 5)
	m.Send(msg)

	doneCh := make(chan bool)

	go func(doneCh chan<- bool) {
		defer cancel()
		wg.Wait()

		doneCh <- true

		return
	}(doneCh)

	var sentCount uint64 = 0

	for i := uint64(0); i < msg.RequiredAcks(); i++ {
		if msg.Acks() == 0 {
			m.Send(msg)
			sentCount++
		} else {
			break
		}
	}

	<-doneCh

	assert.LessOrEqual(t, sentCount, msg.RequiredAcks())

	assert.GreaterOrEqual(t, msg.Acks(), msg.RequiredAcks())
	assert.True(t, msg.AckedBy(singleSub.Id()))

	assertContextCancelledError(t, <-ch)
}

func TestMembroker_pruning(t *testing.T) {
	m, _, cancel, ch := startMembroker(t, 5*time.Second)

	msg := makeMessage(t, 5).SetExpiration(time.Now().Add(3 * time.Second))
	assert.True(t, msg.CanExpire())

	m.Send(msg)

	brokerMsg := m.Message(msg.Id())
	assert.NotNil(t, brokerMsg)
	assert.True(t, brokerMsg.CanExpire())
	assert.False(t, brokerMsg.Expired())

	<-time.After(2 * time.Second)
	assert.False(t, brokerMsg.Expired())

	<-time.After(5 * time.Second)

	cancel()

	assert.Nil(t, m.Message(msg.Id()))
	assert.True(t, msg.Expired())
	assertContextCancelledError(t, <-ch)
}

func TestMembroker_Subscribe(t *testing.T) {
	m, _, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	_, validSub := newSubscriber(t, m)
	if !validSub {
		return
	}

	cancel()

	assertContextCancelledError(t, <-ch)
}

func TestMembroker_Unsubscribe(t *testing.T) {
	m, _, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	sub, validSub := newSubscriber(t, m)
	if !validSub {
		return
	}

	m.Unsubscribe(sub.Id())
	cancel()

	assert.True(t, sub.Closed())

	assertContextCancelledError(t, <-ch)
}

func TestMembroker_Stop(t *testing.T) {
	m, _, cancel, ch := startMembroker(t, defaultPrune)
	defer cancel()

	m.Stop()

	assert.NoError(t, <-ch)
}

// func TestMembroker_exitSignal(t *testing.T) {
// 	t.Skip("screw it")
//
// 	_, _, cancel, ch := startMembroker(t)
// 	defer cancel()
//
// 	killWithInterrupt(t)
// 	assertContextCancelledError(t, <-ch)
// }
//
// func killWithInterrupt(t *testing.T) {
// 	t.Helper()
// 	p, err := os.FindProcess(os.Getpid())
// 	assert.NoError(t, err)
// 	assert.NoError(t, p.Signal(os.Interrupt))
// }

func assertValidSubscriber(t *testing.T, sub *membroker.Subscriber[string]) bool {
	return assert.NotNil(t, sub) && assert.NotEmpty(t, sub.Id())
}

func assertErrorIs(t *testing.T, err, target error) {
	if !assert.ErrorIs(t, err, target) {
		t.Errorf("got unexpected error %v", err)
	}
}

func assertContextCancelledError(t *testing.T, err error) {
	assertErrorIs(t, err, context.Canceled)
}

func ackMsg(ctx context.Context, s *membroker.Subscriber[string], wg *sync.WaitGroup) {
	ackMsgHelper(ctx, s, wg, nil, nil)
}

func ackBreakAfter(ctx context.Context, s *membroker.Subscriber[string], wg *sync.WaitGroup, breakAfter int) {
	ackMsgHelper(ctx, s, wg, &breakAfter, nil)
}

func ackAfterReceives(ctx context.Context, s *membroker.Subscriber[string], wg *sync.WaitGroup, ackAfter int) {
	ackMsgHelper(ctx, s, wg, nil, &ackAfter)
}

func ackMsgHelper(ctx context.Context, s *membroker.Subscriber[string], wg *sync.WaitGroup, breakAfter, ackAfter *int) {
	defer wg.Done()

	recvAll := s.RecvAll()

	var ackCount = 0

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-recvAll:
			if ackAfter == nil && breakAfter == nil {
				msg.Ack(s.Id())
				return
			}

			if ackAfter != nil {
				if ackCount == *ackAfter {
					msg.Ack(s.Id())
					return
				}

				ackCount++
				continue
			}

			if breakAfter != nil {
				msg.Ack(s.Id())
				ackCount++

				if ackCount == *breakAfter {
					return
				}

				continue
			}
		}
	}
}
