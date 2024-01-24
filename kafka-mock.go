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
		mu           sync.Mutex
		parentLane   lane.Lane
		l            lane.Lane
		wg           sync.WaitGroup
		cancelFn     context.CancelFunc
		started      atomic.Bool
		stopped      atomic.Bool
		initializing sync.WaitGroup
		serverPort   uint
		listener     net.Listener
		clients      map[int]*kafkaClient
		active       sync.WaitGroup
		ds           *kafkaDataStore
		latency      time.Duration
	}
)

func NewKafkaMock(l lane.Lane, serverPort uint) *KafkaMock {
	initializeApis()

	return &KafkaMock{
		parentLane: l,
		l:          l,
		serverPort: serverPort,
		ds:         newKafkaDataStore(),
	}
}

// Starts the kafka mock server
func (km *KafkaMock) Start() {
	km.mu.Lock()
	defer km.mu.Unlock()

	if km.started.Swap(true) {
		panic("can't start more than once")
	}

	km.listener = nil
	km.clients = map[int]*kafkaClient{}
	km.stopped.Store(false)

	l, cancelFn := km.parentLane.DeriveWithCancel()
	km.l = l
	km.cancelFn = cancelFn

	km.wg.Add(1)
	km.initializing.Add(1)
	go km.run()
}

func (km *KafkaMock) RequestStop() {
	alreadyStopped := km.stopped.Swap(true)
	if !alreadyStopped {
		if km.started.Load() {
			// wait for startup to complete
			km.l.Trace("request stop is ensuring startup completed first")
			km.initializing.Wait()

			// force-close the socket listener
			km.mu.Lock()
			if km.listener != nil {
				km.listener.Close()
				km.l.Trace("kafka mock server listener terminated")
				km.listener = nil
			}
			km.mu.Unlock()

			// cancel any i/o tasks
			km.cancelFn()
		}
	}
}

func (km *KafkaMock) WaitForTermination() {
	km.l.Trace("waiting for kafka mock to terminate")
	km.wg.Wait()
	km.RequestStop() // releases resources if not already released
	km.l.Trace("kafka mock terminated")
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

func (km *KafkaMock) run() {
	defer func() {
		km.active.Wait()
		km.started.Store(false)
		km.wg.Done()
	}()

	// establish socket service
	iface := fmt.Sprintf(":%d", km.serverPort)
	listener, err := net.Listen("tcp", iface)
	if err != nil {
		panic(fmt.Sprintf("error opening kafka mock server socker: %v", err))
	}

	km.listener = listener
	cxnNumber := 0

	km.l.Tracef("kafka mock server is listening on %s", iface)
	km.initializing.Done()

	for {
		connection, err := listener.Accept()
		if err != nil {
			if !wasSocketClosed(err) {
				panic(fmt.Sprintf("accept error: %v", err))
			}
			break
		}

		select {
		case <-km.l.Done():
			// don't accept more connections
			connection.Close()
			continue
		default:
			// continue
		}

		km.l.Tracef("client connected: %s <-> %s", connection.LocalAddr().String(), connection.RemoteAddr().String())

		km.mu.Lock()
		cxnNumber++
		kc := newKafkaClient(km.l, km.ds, connection, km.serverPort, km.latency, func() {
			km.l.Tracef("client disconnected: %s <-> %s", connection.LocalAddr().String(), connection.RemoteAddr().String())
			km.mu.Lock()
			delete(km.clients, cxnNumber)
			km.active.Done()
			km.wg.Done()
			km.mu.Unlock()
		})
		km.clients[cxnNumber] = kc
		km.active.Add(1)
		km.wg.Add(1)
		km.mu.Unlock()
	}
}

// Tells the mock to stop accepting new requests and
// to complete any blocked requests.
func (km *KafkaMock) FinishRequests() {
	km.cancelFn()
}

// Stops the server and gracefully closes clients, then starts
// a new server with the same data store.
func (km *KafkaMock) Restart() {
	km.RequestStop()
	km.WaitForTermination()
	km.Start()
	km.initializing.Wait()
}

// Clients will usually expect partitions to pre-exist before they connect.
func (km *KafkaMock) CreatePartitionTopics(topics []string, partition int) {
	for _, topic := range topics {
		kt := km.ds.createTopic(topic)
		kt.createPartition(int32(partition))
	}
}

// Directly manipulate the offset of a consumer group
func (km *KafkaMock) SetConsumerGroupOffset(topic string, partition int, group string, offset int64) {
	kt := km.ds.getTopic(topic)
	if kt != nil {
		kp := kt.getPartition(int32(partition))
		if kp != nil {
			kp.mu.Lock()
			defer kp.mu.Unlock()

			kp.GroupCommittedOffsets[group] = offset
		}
	}
}
