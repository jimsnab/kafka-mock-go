package kafkamock

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jimsnab/go-lane"
)

type (
	KafkaMock struct {
		mu       sync.Mutex
		l        lane.Lane
		starting sync.WaitGroup
		wg       sync.WaitGroup
		cancelFn context.CancelFunc
		stopped  atomic.Bool
		port     int
		listener net.Listener
		clients  map[int]*kafkaClient
		ds       *kafkaDataStore
	}
)

func NewKafkaMock(l lane.Lane, port int) *KafkaMock {
	initializeApis()
	return &KafkaMock{
		l:       l,
		port:    port,
		clients: map[int]*kafkaClient{},
		ds:      newKafkaDataStore(),
	}
}

// Starts the kafka mock server
func (km *KafkaMock) Start() {
	km.mu.Lock()
	defer km.mu.Unlock()

	if km.cancelFn != nil {
		panic("can't start more than once")
	}

	l2, cancelFn := km.l.DeriveWithCancel()
	km.cancelFn = cancelFn
	km.wg.Add(1)
	km.starting.Add(1)
	go km.run(l2)
}

func (km *KafkaMock) RequestStop() {
	alreadyStopped := km.stopped.Swap(true)
	if !alreadyStopped {
		km.mu.Lock()
		defer km.mu.Unlock()

		// wait for startup to complete
		km.starting.Wait()

		if km.cancelFn != nil {
			km.cancelFn()
		}

		if km.listener != nil {
			km.listener.Close()
			km.l.Trace("kafka mock server listener terminated")
			km.listener = nil
		}

		for _, client := range km.clients {
			km.l.Tracef("kafka mock server closing client %s", client.String())
			client.Close()
		}
	}
}

func (km *KafkaMock) WaitForTermination() {
	km.wg.Wait()
	km.RequestStop() // releases resources if not already released
}

func (km *KafkaMock) SimplePost(topic string, partition int, key, value []byte) {
	kt := km.ds.createTopic(topic)
	kp := kt.createPartition(int32(partition))
	kp.postRecord(0, time.Now(), key, value, nil)
}

func (km *KafkaMock) ExtendedPost(topic string, partition int, key, value []byte, headers map[string][]byte, timestamp time.Time) {
	kt := km.ds.createTopic(topic)
	kp := kt.createPartition(int32(partition))
	kp.postRecord(0, timestamp, key, value, headers)
}

func (km *KafkaMock) run(l lane.Lane) {
	defer km.wg.Done()

	// establish socket service
	iface := fmt.Sprintf(":%d", km.port)
	listener, err := net.Listen("tcp", iface)
	if err != nil {
		panic(fmt.Sprintf("error opening kafka mock server socker: %v", err))
	}

	km.listener = listener
	cxnNumber := 0

	l.Tracef("kafka mock server is listening on %s", iface)
	km.starting.Done()

	for {
		connection, err := listener.Accept()
		if err != nil {
			if !wasSocketClosed(err) {
				panic(fmt.Sprintf("accept error: %v", err))
			}
			break
		}

		l.Tracef("client connected: %s <-> %s", connection.LocalAddr().String(), connection.RemoteAddr().String())

		km.mu.Lock()
		cxnNumber++
		kc := newKafkaClient(l, km.ds, connection, km.port, func() {
			l.Tracef("client disconnected: %s <-> %s", connection.LocalAddr().String(), connection.RemoteAddr().String())
			km.mu.Lock()
			delete(km.clients, cxnNumber)
			km.wg.Done()
			km.mu.Unlock()
		})
		km.clients[cxnNumber] = kc
		km.wg.Add(1)
		km.mu.Unlock()
	}
}
