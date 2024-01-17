package kafkamock

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/segmentio/kafka-go"
)

func testCreateKafkaMockServer(t *testing.T, serverPort uint) (tl lane.TestingLane, mock *KafkaMock) {
	tl = lane.NewTestingLane(context.Background())
	tl.WantDescendantEvents(true)
	ll := lane.NewLogLane(tl)
	ll.Logger().SetFlags(ll.Logger().Flags() | log.Ldate | log.Ltime | log.Lmicroseconds)
	tl.AddTee(ll)

	mock = NewKafkaMock(tl, serverPort)

	mock.Start()
	return
}

func testStopMockServer(t *testing.T, mock *KafkaMock) {
	mock.RequestStop()
	mock.WaitForTermination()
}

func testKafkaConnect(t *testing.T, serverPort uint, topics []string) (reader *kafka.Reader) {
	return testKafkaConnectEx(t, serverPort, topics, time.Millisecond*10, 0, 10e6)
}

func testKafkaConnectEx(t *testing.T, serverPort uint, topics []string, commitInterval, heartbeatInterval time.Duration, maxBytes int) (reader *kafka.Reader) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           []string{fmt.Sprintf("localhost:%d", serverPort)},
		GroupID:           "kafka-mock",
		GroupTopics:       topics,
		Partition:         0,
		MaxBytes:          maxBytes,
		CommitInterval:    commitInterval,
		HeartbeatInterval: heartbeatInterval,
	})

	reader = r
	return
}

func testCloseKafkaReader(t *testing.T, reader *kafka.Reader, mock *KafkaMock) {
	// if the test has exactly one client connected, close it gracefully
	var clients []*kafkaClient
	if mock != nil {
		mock.mu.Lock()
		clients = make([]*kafkaClient, 0, len(mock.clients))
		for _, client := range mock.clients {
			clients = append(clients, client)
		}
		mock.mu.Unlock()
	}

	// disconnect the client
	if err := reader.Close(); err != nil {
		t.Fatalf("kafka-feed: read close error: %v", err)
	}

	// wait for server to drop the client also
	for _, client := range clients {
		fmt.Printf("kafka-feed: waiting for client %d\n", client.clientPort)
		client.connected.Wait()
	}
}

func TestKafkaHeader(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	r := testKafkaConnect(t, 21001, []string{"topic-a"})
	defer testCloseKafkaReader(t, r, mock)

	// spin until expected api is invoked - upon failure, the test times out
	for {
		if strings.Contains(tl.EventsToString(), "FindCoordinator") {
			break
		}

		time.Sleep(time.Millisecond)
	}
}

func TestKafkaReadOne(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test"))

	r := testKafkaConnect(t, 21001, []string{"topic-a"})
	defer testCloseKafkaReader(t, r, mock)

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
}

func TestKafkaCommitOne(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test"))

	r := testKafkaConnect(t, 21001, []string{"topic-a"})
	defer testCloseKafkaReader(t, r, mock)

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	r.CommitMessages(tl, m)

	tl.Infof("committed: %v", m)
}

func TestKafkaReadTwo(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test 1"))
	mock.SimplePost("topic-a", 2, nil, []byte("test 2"))

	r := testKafkaConnect(t, 21001, []string{"topic-a"})
	defer testCloseKafkaReader(t, r, mock)

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)

	m, err = r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
}

func TestKafkaReadTwoTwice(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test 1"))
	mock.SimplePost("topic-a", 2, nil, []byte("test 2"))

	r := testKafkaConnect(t, 21001, []string{"topic-a"})

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
	if string(m.Value) != "test 1" {
		t.Error("incorrect first value")
	}

	m, err = r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
	if string(m.Value) != "test 2" {
		t.Error("incorrect second value")
	}

	testCloseKafkaReader(t, r, mock)

	r = testKafkaConnect(t, 21001, []string{"topic-a"})

	m, err = r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
	if string(m.Value) != "test 1" {
		t.Error("incorrect first value #2")
	}

	m, err = r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
	if string(m.Value) != "test 2" {
		t.Error("incorrect second value #2")
	}

	testCloseKafkaReader(t, r, mock)
}

func TestKafkaReadTwoRepeatOne(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test 1"))
	mock.SimplePost("topic-a", 2, nil, []byte("test 2"))

	r := testKafkaConnect(t, 21001, []string{"topic-a"})

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
	if string(m.Value) != "test 1" {
		t.Error("incorrect first value")
	}

	r.CommitMessages(tl, m)

	m, err = r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
	if string(m.Value) != "test 2" {
		t.Error("incorrect second value")
	}

	testCloseKafkaReader(t, r, mock)

	r = testKafkaConnect(t, 21001, []string{"topic-a"})

	m, err = r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
	if string(m.Value) != "test 2" {
		t.Error("incorrect first value #2")
	}

	testCloseKafkaReader(t, r, mock)
}

func TestKafkaCommitThousands(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	for n := 0; n < 10000; n++ {
		mock.SimplePost("topic-a", 2, []byte(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("testing: test %d", n)))
	}

	r := testKafkaConnect(t, 21001, []string{"topic-a"})
	defer testCloseKafkaReader(t, r, mock)

	for n := 0; n < 10000; n++ {
		m, err := r.FetchMessage(tl)
		if err != nil {
			t.Fatalf("kafka-feed: read message error: %v", err)
		}

		r.CommitMessages(tl, m)
	}

	tl.Infof("committed 10000 messages")
}

func TestKafkaCommitHundredThousand(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	for n := 0; n < 100000; n++ {
		mock.SimplePost("topic-a", 2, []byte(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("testing: test %d", n)))
	}

	r := testKafkaConnect(t, 21001, []string{"topic-a"})
	defer testCloseKafkaReader(t, r, mock)

	for n := 0; n < 100000; n++ {
		m, err := r.FetchMessage(tl)
		if err != nil {
			t.Fatalf("kafka-feed: read message error: %v", err)
		}

		r.CommitMessages(tl, m)
	}

	tl.Infof("committed 100000 messages")
}

func TestKafkaCommitHundredSyncCommits(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	for n := 0; n < 100; n++ {
		mock.SimplePost("topic-a", 2, []byte(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("testing: test %d", n)))
	}

	r := testKafkaConnectEx(t, 21001, []string{"topic-a"}, 0, 0, 1024)
	defer testCloseKafkaReader(t, r, mock)

	for n := 0; n < 100; n++ {
		m, err := r.FetchMessage(tl)
		if err != nil {
			t.Fatalf("kafka-feed: read message error: %v", err)
		}

		r.CommitMessages(tl, m)
	}

	tl.Infof("committed 100 messages")
}

func TestKafkaCommitHundredLatency(t *testing.T) {
	// when latency is introduced, the client's requests can get combined
	// latency of 50 ms is specifically chosen to collide with a client
	// side heartbeat
	tl, mock := testCreateKafkaMockServer(t, 21001)
	mock.latency = time.Millisecond * 50
	defer testStopMockServer(t, mock)

	for n := 0; n < 100; n++ {
		mock.SimplePost("topic-a", 2, []byte(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("testing: test %d", n)))
	}

	r := testKafkaConnectEx(t, 21001, []string{"topic-a"}, 0, 0, 1024)
	defer testCloseKafkaReader(t, r, mock)

	msgs := make([]*kafka.Message, 0, 100)
	for n := 0; n < 100; n++ {
		m, err := r.FetchMessage(tl)
		if err != nil {
			t.Fatalf("kafka-feed: read message error: %v", err)
		}
		msgs = append(msgs, &m)
	}

	for _, m := range msgs {
		r.CommitMessages(tl, *m)
	}

	tl.Infof("committed 100 messages")
}
