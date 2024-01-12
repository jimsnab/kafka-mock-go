package kafkamock

import (
	"bufio"
)

type (
	leaveGroupResponseV0 struct {
		ErrorCode int16
	}
)

func leaveGroupV0(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error) {
	response = &leaveGroupResponseV0{}
	return
}
