package kafkamock

import (
	"bufio"
	"bytes"
	"errors"
)

type (
	syncGroupRequestV0 struct {
		GroupId      string
		GenerationId int32
		MemberId     string
		Assignments  []syncGroupRequestAssignmentV0
	}

	syncGroupRequestAssignmentV0 struct {
		MemberID    string
		Assignments []byte
	}

	memberAssignment struct {
		Version              int16
		PartitionAssignments []memberPartitionAssignment
		UserData             []byte
	}

	memberPartitionAssignment struct {
		Topic      string
		Partitions []int32
	}

	syncGroupResponseV0 struct {
		ErrorCode   int16
		Assignments []byte
	}
)

func syncGroupV0(reader *bufio.Reader, kc *kafkaClient, kmh *kafkaMessageHeader) (response any, rtags map[int]any, err error) {
	request, err := readRequest[syncGroupRequestV0](reader)
	if err != nil {
		return
	}

	if len(request.Assignments) != 0 {
		err = errors.New("mock server is the leader - assignments in sync_group request are not supported")
		return
	}

	response = &syncGroupResponseV0{
		Assignments: makeMemberAssignment(kc.ds),
	}
	return
}

func makeMemberAssignment(ds *kafkaDataStore) []byte {
	// assign all topics & partitions to this client
	mpas := make([]memberPartitionAssignment, 0, len(ds.Topics))
	for name, topic := range ds.Topics {
		pars := make([]int32, 0, len(topic.Partitions))
		for _, par := range topic.Partitions {
			pars = append(pars, par.Index)
		}

		mpas = append(mpas, memberPartitionAssignment{Topic: name, Partitions: pars})
	}

	a := memberAssignment{
		Version:              1,
		PartitionAssignments: mpas,
	}

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	encodeObject(w, a)
	w.Flush()

	return buf.Bytes()
}
