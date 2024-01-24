package kafkamock

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/segmentio/kafka-go"
)

func testCreateKafkaMockServer(t *testing.T, serverPort uint, topics []string) (tl lane.TestingLane, mock *KafkaMock) {
	// tl is used by the tests to check for specific log messages to confirm points of execution
	tl = lane.NewTestingLane(context.Background())
	tl.WantDescendantEvents(true)

	// ll is a development convenience to be able to see the log spew
	ll := lane.NewLogLane(tl)
	ll.Logger().SetFlags(ll.Logger().Flags() | log.Ldate | log.Ltime | log.Lmicroseconds)
	tl.AddTee(ll)

	mock = NewKafkaMock(tl, serverPort)
	mock.CreatePartitionTopics(topics, 2)

	mock.Start()
	return
}

func loadTestEvents(t *testing.T, file string, limit int) [][]byte {
	content, err := os.ReadFile(file)
	if err != nil {
		return nil
	}

	lines := strings.Split(string(content), "\n")
	events := make([][]byte, 0, len(lines))
	for _, line := range lines {
		content := []byte(strings.TrimSpace(line))
		if len(content) == 0 {
			continue
		}

		if limit >= 0 {
			limit--
			if limit < 0 {
				break
			}
		}

		events = append(events, content)
	}

	return events
}

func testStopMockServer(t *testing.T, mock *KafkaMock) {
	mock.RequestStop()
	mock.WaitForTermination()
}

func testKafkaConnect(t *testing.T, serverPort uint, topics []string) (reader *kafka.Reader) {
	return testKafkaConnectEx(t, serverPort, topics, time.Millisecond*10, 0, 10e6, 0)
}

func testKafkaConnectEx(t *testing.T, serverPort uint, topics []string, commitInterval, heartbeatInterval time.Duration, maxBytes int, brt time.Duration) (reader *kafka.Reader) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           []string{fmt.Sprintf("localhost:%d", serverPort)},
		GroupID:           "kafka-mock",
		GroupTopics:       topics,
		Partition:         0,
		MaxBytes:          maxBytes,
		CommitInterval:    commitInterval,
		HeartbeatInterval: heartbeatInterval,
		ReadBatchTimeout:  brt,
		MaxWait:           brt,
	})

	reader = r
	return
}

func testCloseKafkaReader(t *testing.T, tl lane.Lane, reader *kafka.Reader) {
	tl.Info("kafka-feed: closing kafka reader")

	// disconnect the client
	if err := reader.Close(); err != nil {
		t.Fatalf("kafka-feed: read close error: %v", err)
	}

	tl.Info("kafka-feed: kafka reader closed")
}

func TestKafkaHeader(t *testing.T) {
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	r := testKafkaConnect(t, 21001, topics)
	defer testCloseKafkaReader(t, tl, r)

	// spin until expected api is invoked - upon failure, the test times out
	for {
		if strings.Contains(tl.EventsToString(), "FindCoordinator") {
			break
		}

		time.Sleep(time.Millisecond)
	}
}

func TestKafkaReadOne(t *testing.T) {
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test"))

	r := testKafkaConnect(t, 21001, topics)
	defer testCloseKafkaReader(t, tl, r)
	defer mock.FinishRequests()

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	tl.Infof("fetched: %v", m)
}

func TestKafkaCommitOne(t *testing.T) {
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test"))

	r := testKafkaConnect(t, 21001, topics)
	defer testCloseKafkaReader(t, tl, r)
	defer mock.FinishRequests()

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	r.CommitMessages(tl, m) // asynchronous

	for {
		if strings.Contains(tl.EventsToString(), "cmd OffsetCommit v2") {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	tl.Infof("committed: %v", m)
}

func TestKafkaReadTwo(t *testing.T) {
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test 1"))
	mock.SimplePost("topic-a", 2, nil, []byte("test 2"))

	r := testKafkaConnect(t, 21001, topics)
	defer testCloseKafkaReader(t, tl, r)
	defer mock.FinishRequests()

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
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test 1"))
	mock.SimplePost("topic-a", 2, nil, []byte("test 2"))

	func() {
		r := testKafkaConnect(t, 21001, topics)
		defer testCloseKafkaReader(t, tl, r)

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

		mock.FinishRequests()
		mock.active.Wait()
	}()

	func() {
		mock.Restart()

		r := testKafkaConnect(t, 21001, topics)
		defer testCloseKafkaReader(t, tl, r)
		defer mock.FinishRequests()

		m, err := r.FetchMessage(tl)
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
	}()
}

func TestKafkaReadTwoRepeatOne(t *testing.T) {
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test 1"))
	mock.SimplePost("topic-a", 2, nil, []byte("test 2"))

	func() {
		r := testKafkaConnect(t, 21001, topics)
		defer testCloseKafkaReader(t, tl, r)

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

		mock.FinishRequests()
		mock.active.Wait()
	}()

	func() {
		mock.Restart()

		r := testKafkaConnect(t, 21001, topics)
		defer testCloseKafkaReader(t, tl, r)
		defer mock.FinishRequests()

		m, err := r.FetchMessage(tl)
		if err != nil {
			t.Fatalf("kafka-feed: read message error: %v", err)
		}

		tl.Infof("fetched: %v", m)
		if string(m.Value) != "test 2" {
			t.Error("incorrect first value #2")
		}
	}()
}

func TestKafkaCommitThousands(t *testing.T) {
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	for n := 0; n < 10000; n++ {
		mock.SimplePost("topic-a", 2, []byte(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("testing: test %d", n)))
	}

	r := testKafkaConnect(t, 21001, topics)
	defer testCloseKafkaReader(t, tl, r)
	defer mock.FinishRequests()

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
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	for n := 0; n < 100000; n++ {
		mock.SimplePost("topic-a", 2, []byte(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("testing: test %d", n)))
	}

	r := testKafkaConnect(t, 21001, topics)
	defer testCloseKafkaReader(t, tl, r)
	defer mock.FinishRequests()

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
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	for n := 0; n < 100; n++ {
		mock.SimplePost("topic-a", 2, []byte(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("testing: test %d", n)))
	}

	r := testKafkaConnectEx(t, 21001, topics, 0, 0, 1024, 0)
	defer testCloseKafkaReader(t, tl, r)
	defer mock.FinishRequests()

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
	// latency of 30 ms is specifically chosen to collide with a client
	// side heartbeat
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	mock.latency = time.Millisecond * 30
	defer testStopMockServer(t, mock)

	for n := 0; n < 100; n++ {
		mock.SimplePost("topic-a", 2, []byte(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("testing: test %d", n)))
	}

	r := testKafkaConnectEx(t, 21001, topics, 0, 0, 1024, 0)
	defer testCloseKafkaReader(t, tl, r)
	defer mock.FinishRequests()

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

func TestKafkaRapidTwoClients(t *testing.T) {
	topics := []string{"simple-feed"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	mock.latency = time.Millisecond * 5
	defer testStopMockServer(t, mock)

	events := loadTestEvents(t, "test_assets/simple-feed.events", -1)
	for i := 0; i < 30; i++ {
		for _, event := range events {
			mock.SimplePost("simple-feed", 2, []byte("unused-key"), event)
		}
	}

	pullAndDrop := func() {
		r := testKafkaConnectEx(t, 21001, topics, 0, 0, 1024, 0)
		defer testCloseKafkaReader(t, tl, r)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.FetchMessage(tl)
			if err != nil {
				tl.Infof("fetch error: %v", err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			mock.mu.Lock()
			defer mock.mu.Unlock()
			for _, client := range mock.clients {
				client.conn.Close()
			}
		}()

		wg.Wait()
	}

	for i := 0; i < 30; i++ {
		pullAndDrop()
	}
}

func TestKafkaRapidFetchAndHeartbeat(t *testing.T) {
	topics := []string{"simple-feed"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	mock.latency = time.Millisecond * 5
	defer testStopMockServer(t, mock)

	r := testKafkaConnectEx(t, 21001, topics, 0, time.Millisecond*10, 1024, 0)
	defer testCloseKafkaReader(t, tl, r)

	events := loadTestEvents(t, "test_assets/simple-feed.events", -1)
	for _, event := range events {
		mock.SimplePost("simple-feed", 2, []byte("unused-key"), event)
	}

	end := time.Now().Add(time.Second)
	for {
		if time.Now().After(end) {
			break
		}

		_, err := r.FetchMessage(tl)
		if err != nil {
			t.Fatalf("fetch error: %v", err)
		}

		mock.SimplePost("simple-feed", 2, []byte("unused-key"), events[0])
	}

}

func TestKafkaMessagesAfterStart(t *testing.T) {
	topics := []string{"simple-feed"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	mock.latency = time.Millisecond * 5
	defer testStopMockServer(t, mock)

	r := testKafkaConnectEx(t, 21001, topics, 0, 0, 1024, 0)
	defer testCloseKafkaReader(t, tl, r)

	time.Sleep(time.Millisecond * 250)
	tl.Info("filling with some events")

	events := loadTestEvents(t, "test_assets/simple-feed.events", -1)
	for _, event := range events {
		mock.SimplePost("simple-feed", 2, []byte("unused-key"), event)
	}

	for _, event := range events {
		m, err := r.FetchMessage(tl)
		if err != nil {
			t.Fatalf("fetch error: %v", err)
		}

		if string(m.Value) != string(event) {
			t.Errorf("payload mismatch\n\nreceived: %s\n\nexpected: %s", string(m.Value), string(event))
		}
	}

	tl.Info("pulled all the events")
	mock.FinishRequests()
}

func TestKafkaCommitOneGoBack(t *testing.T) {
	topics := []string{"topic-a"}
	tl, mock := testCreateKafkaMockServer(t, 21001, topics)
	defer testStopMockServer(t, mock)

	mock.SimplePost("topic-a", 2, nil, []byte("test"))

	r := testKafkaConnectEx(t, 21001, topics, 0, 0, 1024, time.Millisecond*50)

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}

	r.CommitMessages(tl, m) // asynchronous

	for {
		if strings.Contains(tl.EventsToString(), "cmd OffsetCommit v2") {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	tl.Infof("committed: %v", m)

	// reopen client back at offset 0
	testCloseKafkaReader(t, tl, r)
	mock.SetConsumerGroupOffset(topics[0], 2, "kafka-mock", 0)
	r = testKafkaConnectEx(t, 21001, topics, 0, 0, 1024, time.Millisecond*50)

	m, err = r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read message error: %v", err)
	}
	if m.Offset != 0 {
		t.Error("wrong offset")
	}

	mock.FinishRequests()
	testCloseKafkaReader(t, tl, r)
}
