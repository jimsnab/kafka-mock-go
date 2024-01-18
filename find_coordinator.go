package kafkamock

import (
	"bufio"
)

type (
	findCoordinatorRequestV0 struct {
		Key string
	}

	findCoordinatorResponseV0 struct {
		ErrorCode int16
		NodeId    int32
		Host      string
		Port      int32
	}
)

func findCoordinatorV0(reader *bufio.Reader, kc *kafkaClient, kmh *kafkaMessageHeader) (response any, rtags map[int]any, err error) {
	_, err = readRequest[findCoordinatorRequestV0](reader)
	if err != nil {
		return
	}

	response = &findCoordinatorResponseV0{
		NodeId: kLeaderNode,
		Host:   "localhost",
		Port:   int32(kc.serverPort),
	}
	return
}
