package kafkamock

import "bufio"

type (
	offsetCommitRequestV2 struct {
		GroupId                   string
		GenerationIdOrMemberEpoch int32
		MemberId                  string
		RetentionTimeMs           int64
		Topics                    []offsetCommitTopic
	}

	offsetCommitTopic struct {
		Name       string
		Partitions []offsetCommitPartition
	}

	offsetCommitPartition struct {
		PartitionIndex    int32
		CommittedOffset   int64
		CommittedMetadata NullableString
	}

	offsetCommitResponseV2 struct {
		Topics []offsetCommitResponseTopic
	}

	offsetCommitResponseTopic struct {
		Name       string
		Partitions []offsetCommitResponsePartition
	}

	offsetCommitResponsePartition struct {
		PartitionIndex int32
		ErrorCode      int16
	}
)

func offsetCommitV2(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error) {
	request, err := readRequest[offsetCommitRequestV2](reader)
	if err != nil {
		return
	}

	rtopics := make([]offsetCommitResponseTopic, 0, len(request.Topics))

	for _, topic := range request.Topics {
		kt := kc.ds.getTopic(topic.Name)

		rtopic := offsetCommitResponseTopic{
			Name:       topic.Name,
			Partitions: make([]offsetCommitResponsePartition, 0, len(topic.Partitions)),
		}

		for _, par := range topic.Partitions {
			rpar := offsetCommitResponsePartition{
				PartitionIndex: par.PartitionIndex,
			}

			var kp *kafkaPartition
			if kt != nil {
				kp = kt.getPartition(par.PartitionIndex)
			}
			if kp != nil {
				kp.lock()
				offset := kp.GroupCommittedOffsets[request.GroupId]
				if offset < par.CommittedOffset {
					kp.GroupCommittedOffsets[request.GroupId] = par.CommittedOffset
					kc.l.Tracef("kafka offset moved to %d", par.CommittedOffset)
				}
				kp.unlock()
			} else {
				rpar.ErrorCode = int16(UnknownTopicOrPartition)
			}
			rtopic.Partitions = append(rtopic.Partitions, rpar)
		}

		rtopics = append(rtopics, rtopic)
	}

	ocv2 := offsetCommitResponseV2{
		Topics: rtopics,
	}
	response = &ocv2
	return
}
