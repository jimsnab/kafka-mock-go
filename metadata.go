package kafkamock

import (
	"bufio"
)

type (
	metadataRequestV1 struct {
		Topics []string
	}

	metadataResponseV1 struct {
		Brokers      []brokersV1
		ControllerId int32
		Topics       []topicsV1
	}

	brokersV1 struct {
		NodeId int32
		Host   string
		Port   int32
		Rack   NullableString
	}

	topicsV1 struct {
		ErrorCode  int16
		Name       string
		IsInternal bool
		Partitions []partitionV1
	}

	partitionV1 struct {
		ErrorCode      int16
		PartitionIndex int32
		LeaderId       int32
		ReplicaNodes   []int32
		IsrNodes       []int32
	}
)

const kLeaderNode = 100

func metadataV1(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error) {
	request, err := readRequest[metadataRequestV1](reader)
	if err != nil {
		return
	}

	topics := make([]topicsV1, 0, len(request.Topics))
	for _,t := range request.Topics {
		topics = append(topics, topicsV1{Name: t, Partitions: []partitionV1{{PartitionIndex: 2, LeaderId: kLeaderNode, ReplicaNodes: []int32{1}, IsrNodes: []int32{1}}}})
	}

	response = &metadataResponseV1{
		Brokers: []brokersV1{
			{NodeId: kLeaderNode, Host: "localhost", Port: int32(kc.port)},
		},
		ControllerId: 500,
		Topics: topics,
	}
	return
}
