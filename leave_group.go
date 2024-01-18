package kafkamock

import (
	"bufio"
)

type (
	leaveGroupRequestV0 struct {
		GroupId  string
		MemberId string
	}

	leaveGroupResponseV0 struct {
		ErrorCode int16
	}
)

func leaveGroupV0(reader *bufio.Reader, kc *kafkaClient, kmh *kafkaMessageHeader) (response any, rtags map[int]any, err error) {
	if _, err = readRequest[leaveGroupRequestV0](reader); err != nil {
		return
	}

	response = &leaveGroupResponseV0{}
	return
}
