package kafkamock

import (
	"bufio"
)

type (
	offsetFetchRequestV1 struct {
		GroupId string
		Topics []offsetFetchRequestTopic
	}

	offsetFetchRequestTopic struct {
		Name string
		PartitionIndexes []int32
	}

	offsetFetchResponseV1 struct {
		Topics []offsetFetchTopicV1
	}

	offsetFetchTopicV1 struct {
		Name       string
		Partitions []offsetFetchPartitionV1
	}

	offsetFetchPartitionV1 struct {
		PartitionIndex  int32
		CommittedOffset int64
		Metadata        NullableString
		ErrorCode       int16
	}
)

func offsetFetchV1(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error) {
	request, err := readRequest[offsetFetchRequestV1](reader)
	if err != nil {
		return
	}

	data := map[string][]*kafkaPartition{}
	for _,rt := range request.Topics {
		kt := kc.ds.getTopic(rt.Name)
		if kt == nil {
			continue
		}

		for _,pi := range rt.PartitionIndexes {
			par := kt.getPartition(pi)
			if par != nil {
				kps, exists := data[rt.Name]
				if !exists {
					kps = []*kafkaPartition{}
				}
				kps = append(kps, par)
				data[rt.Name] = kps
			}
		}
	}

	rtopics := make([]offsetFetchTopicV1, 0, len(data))
	for name, kps := range data {
		rpars := make([]offsetFetchPartitionV1, 0, len(kps))
		for _,kp := range kps {
			rpars = append(rpars, offsetFetchPartitionV1{
				PartitionIndex: kp.Index,
				CommittedOffset: kp.groupCommittedOffset(request.GroupId),
				ErrorCode: kp.ErrorCode,
				Metadata: kp.Metadata,
			})
		}
		rtopics = append(rtopics, offsetFetchTopicV1{Name: name, Partitions: rpars})
	}

	response = rtopics
	return
}
