package kafkamock

import (
	"bufio"
	"time"
)

type (
	fetchRequestV2 struct {
		ReplicaId int32
		MaxWaitMs int32
		MinBytes  int32
		Topics    []fetchTopic
	}

	fetchTopic struct {
		Topic      string
		Partitions []fetchPartition
	}

	fetchPartition struct {
		Partition         int32
		FetchOffset       int64
		PartitionMaxBytes int32
	}

	fetchResponseV2 struct {
		ThrottleTimeMs int32
		Responses      []fetchResponse
	}

	fetchResponse struct {
		Topic      string
		Partitions []fetchResponsePartition
	}

	fetchResponsePartition struct {
		PartitionIndex int32
		ErrorCode      int16
		HighWatermark  int64
		Records        *messageSetV1
	}

	fetchData struct {
		kp      *kafkaPartition
		offset  int
		maxSize int
		ms      *messageSetV1
	}
)

func fetchV2(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error) {
	request, err := readRequest[fetchRequestV2](reader)
	if err != nil {
		return
	}

	// establish which topics/partitions to fetch
	fds := []*fetchData{}

	for _, topic := range request.Topics {
		kt := kc.ds.getTopic((topic.Topic))

		for _, par := range topic.Partitions {
			var kp *kafkaPartition
			if kt != nil {
				kp = kt.getPartition(par.Partition)
			}

			fd := &fetchData{kp: kp, offset: int(par.FetchOffset), maxSize: int(par.PartitionMaxBytes), ms: newMessageSetV1(par.FetchOffset)}
			fds = append(fds, fd)
		}
	}

	expiration := time.Now().Add(time.Duration(request.MaxWaitMs) * time.Millisecond)
	for {
		more := false
		for _, fd := range fds {
			if fd.kp != nil {
				var rec *kafkaRecord
				fd.kp.lock()
				if fd.offset < len(fd.kp.Records) {
					rec = fd.kp.Records[fd.offset]
				}
				fd.kp.unlock()

				if rec == nil || !fd.ms.appendMessage(rec, fd.maxSize) {
					fd.kp = nil
				} else {
					fd.offset++
					more = true
					expiration = time.Unix(0, 0)
				}
			}
		}

		if !more {
			// is wait time expiring?
			if time.Now().After(expiration) {
				break
			}

			// is client getting closed?
			isClosed := false
			select {
			case <-kc.l.Done():
				isClosed = true
				break
			default:
				break
			}
			if isClosed {
				break
			}

			// did client disconnect?
			if !kc.isConnected() {
				break
			}

			// check for messages again in a moment
			time.Sleep(time.Millisecond * 20)
		}
	}

	frv2 := fetchResponseV2{
		Responses: make([]fetchResponse, 0, len(request.Topics)),
	}

	n := 0
	for _, topic := range request.Topics {
		fd := fds[n]
		n++

		fr := &fetchResponse{
			Topic:      topic.Topic,
			Partitions: []fetchResponsePartition{},
		}

		for _, par := range topic.Partitions {
			fp := fetchResponsePartition{
				PartitionIndex: par.Partition,
				HighWatermark:  int64(fd.offset),
				Records:        fd.ms,
			}

			fr.Partitions = append(fr.Partitions, fp)
		}

		frv2.Responses = append(frv2.Responses, *fr)
	}

	response = frv2
	return
}
