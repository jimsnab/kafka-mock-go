package kafkamock

import (
	"bufio"
)

type (
	heartbeatRequestV0 struct {
		GroupId      string
		GenerationId int32
		MemberId     string
	}

	heartbeatResponseV0 struct {
		ErrorCode int16
	}
)

func heartbeatV0(reader *bufio.Reader, kc *kafkaClient, kmh *kafkaMessageHeader) (response any, rtags map[int]any, err error) {
	_, err = readRequest[heartbeatRequestV0](reader)
	if err != nil {
		return
	}

	response = &heartbeatResponseV0{}
	return
}
