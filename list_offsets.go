package kafkamock

import (
	"bufio"
	"time"
)

type (
	listOffsetsRequestV1 struct {
		ReplicaId int32
		Topics []listOffsetsRequestTopic
	}

	listOffsetsRequestTopic struct {
		Name string
		Parititions []listOffsetsRequestPartitions
	}

	listOffsetsRequestPartitions struct {
		PartitionIndex int32
		Timestamp int64
	}
	
	listOffsetsResponseV1 struct {
		Topics []listOffsetsResponseTopic
	}

	listOffsetsResponseTopic struct {
		Name string
		Partitions []listOffsetResponsePartition
	}

	listOffsetResponsePartition struct {
		PartitionIndex int32
		ErrorCode int16
		Timestamp int64
		Offset int64
	}
)

func listOffsetsV1(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error) {
	request, err := readRequest[listOffsetsRequestV1](reader)
	if err != nil {
		return
	}

	lo := make([]listOffsetsResponseTopic, 0, len(request.Topics))

	for _,t := range request.Topics {
		kt := kc.ds.getTopic(t.Name)
		if kt == nil {
			continue
		}

		rpars := make([]listOffsetResponsePartition, 0, len(t.Parititions))
		for _,p := range t.Parititions {
			kp := kt.getPartition(p.PartitionIndex)
			if kp == nil {
				rpars = append(rpars, listOffsetResponsePartition{PartitionIndex: p.PartitionIndex, ErrorCode: int16(UnknownTopicOrPartition)})
			} else {
				if p.Timestamp == -1 {
					rpars = append(rpars, listOffsetResponsePartition{PartitionIndex: p.PartitionIndex, Timestamp: time.Now().UnixMilli(), Offset: int64(len(kp.Records))})
				} else if p.Timestamp == -2 {
					if len(kp.Records) == 0 {
						rpars = append(rpars, listOffsetResponsePartition{PartitionIndex: p.PartitionIndex})
					} else {
						msg := kp.Records[0]
						rpars = append(rpars, listOffsetResponsePartition{PartitionIndex: p.PartitionIndex, Timestamp: msg.Timestamp})
					}
				} else {
					// slow but this is just a mock
					offset := int64(0)
					ts := int64(0)
					for i := len(kp.Records) - 1 ; i >= 0 ; i-- {
						msg := kp.Records[i]
						ts = msg.Timestamp
						if msg.Timestamp < p.Timestamp {
							offset = int64(i + 1)
							break
						}
					}
					rpars = append(rpars, listOffsetResponsePartition{PartitionIndex: p.PartitionIndex, Timestamp: ts, Offset: offset})
				}
			}
		}

		lo = append(lo, listOffsetsResponseTopic{
			Name: t.Name,
			Partitions: rpars,
		})
	}

	response = lo
	return
}
