package kafkamock

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/segmentio/kafka-go"
)

func testCreateKafkaMockServer(t * testing.T, port int) (tl lane.TestingLane, mock *KafkaMock) {
	tl = lane.NewTestingLane(context.Background())
	tl.WantDescendantEvents(true)
	tl.AddTee(lane.NewLogLane(tl))

	mock = NewKafkaMock(tl, port)

	mock.Start()
	return
}

func testStopMockServer(t *testing.T, mock *KafkaMock) {
	mock.RequestStop()
	mock.WaitForTermination()
}

func testKafkaConnect(t *testing.T, port int, topics []string) (reader *kafka.Reader) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("localhost:%d", port)},
		GroupID: "kafka-mock",
		GroupTopics: topics,
		Partition: 0,
		MaxBytes:  10e6, // 10MB
		CommitInterval: time.Second,
	})

	reader = r
	return
}

func testCloseKafkaReader(t *testing.T, reader *kafka.Reader) {
	if err := reader.Close(); err != nil {
		t.Fatalf("kafka-feed: read close error: %v", err)			
	}
}

func TestKafkaHeader(t *testing.T) {
	tl, mock := testCreateKafkaMockServer(t, 21001)
	defer testStopMockServer(t, mock)

	r := testKafkaConnect(t, 21001, []string{"topic-a"})
	defer testCloseKafkaReader(t, r)

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
	defer testCloseKafkaReader(t, r)

	m, err := r.FetchMessage(tl)
	if err != nil {
		t.Fatalf("kafka-feed: read mesage error: %v", err)
	}

	tl.Infof("fetched: %v", m)
}