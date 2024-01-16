package kafkamock

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestKafkaMockNoStart(t *testing.T) {
	tl := lane.NewTestingLane(context.Background())

	// ensure mock stops if not started
	mock := NewKafkaMock(tl, 21100)
	mock.RequestStop()
	mock.WaitForTermination()
}

func TestKafkaMockNoStart2(t *testing.T) {
	tl := lane.NewTestingLane(context.Background())

	// ensure mock stops if not started and stop not requested
	mock := NewKafkaMock(tl, 21100)
	mock.WaitForTermination()
}

func TestKafkaMockStartStop(t *testing.T) {
	tl := lane.NewTestingLane(context.Background())

	// ensure mock stops immediately after being started
	mock := NewKafkaMock(tl, 21100)
	mock.Start()
	mock.RequestStop()
	mock.WaitForTermination()
}
