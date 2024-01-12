package kafkamock

import (
	"bufio"
)

type (
	joinGroupRequestV1 struct {
		GroupId string
		SessionTimeoutMs int32
		RebalanceTimeoutMs int32
		MemberId string
		ProtocolType string
		Protocols []joinGroupProtocolV1
	}

	joinGroupProtocolV1 struct {
		Name string
		Metadata []byte
	}

	joinGroupResponseV1 struct {
		ErrorCode    int16
		GenerationId int32
		ProtocolName string
		Leader       string
		MemberId     string
		Members      []joinGroupMemberV1
	}

	joinGroupMemberV1 struct {
		MemberId string
		Metadata []byte
	}
)

func joinGroupV1(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error) {
	_, err = readRequest[joinGroupRequestV1](reader)
	if err != nil {
		return
	}

	response = &joinGroupResponseV1{
		ProtocolName: "roundrobin",
		Leader:       "me",
		MemberId:     "1",
		Members: []joinGroupMemberV1{
			{MemberId: "you"},
		},
	}
	return
}
