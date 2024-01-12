package kafkamock

import (
	"sync"
	"time"
)

type (
	kafkaDataStore struct {
		mu     sync.Mutex
		Topics map[string]*kafkaTopic
	}

	kafkaTopic struct {
		mu         sync.Mutex
		Partitions map[int32]*kafkaPartition
	}

	kafkaPartition struct {
		mu                    sync.Mutex
		Index                 int32
		ErrorCode             int16
		Timestamp             int64
		Offset                int64
		Records               []*kafkaRecord
		GroupCommittedOffsets map[string]int64
		Metadata              NullableString
	}

	kafkaRecord struct {
		Attributes int8
		Timestamp  int64
		Key        []byte
		Value      []byte
		Headers    []kafkaHeader
	}

	kafkaHeader struct {
		HeaderKey   string
		HeaderValue []byte
	}
)

func newKafkaDataStore() *kafkaDataStore {
	return &kafkaDataStore{
		Topics: map[string]*kafkaTopic{},
	}
}

func (ds *kafkaDataStore) createTopic(name string) *kafkaTopic {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	topic, exists := ds.Topics[name]
	if !exists {
		topic = &kafkaTopic{
			Partitions: map[int32]*kafkaPartition{},
		}
		ds.Topics[name] = topic
	}
	return topic
}

func (ds *kafkaDataStore) getTopic(name string) *kafkaTopic {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	return ds.Topics[name]
}

func (kp *kafkaTopic) createPartition(number int32) *kafkaPartition {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	partition, exists := kp.Partitions[number]
	if !exists {
		partition = &kafkaPartition{
			Index:                 number,
			Records:               []*kafkaRecord{},
			GroupCommittedOffsets: map[string]int64{},
		}
		kp.Partitions[number] = partition
	}
	return partition
}

func (kp *kafkaTopic) getPartition(number int32) *kafkaPartition {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	return kp.Partitions[number]
}

func (kp *kafkaPartition) lock() {
	kp.mu.Lock()
}

func (kp *kafkaPartition) unlock() {
	kp.mu.Unlock()
}

func (kp *kafkaPartition) postRecord(attribs int8, ts time.Time, key, value []byte, headers map[string][]byte) {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	flatHeaders := make([]kafkaHeader, 0, len(headers))
	for k, v := range headers {
		flatHeaders = append(flatHeaders, kafkaHeader{HeaderKey: k, HeaderValue: v})
	}

	record := &kafkaRecord{
		Attributes: attribs,
		Timestamp:  ts.UnixMilli(),
		Key:        key,
		Value:      value,
		Headers:    flatHeaders,
	}
	kp.Records = append(kp.Records, record)
}

func (kp *kafkaPartition) groupCommittedOffset(group string) int64 {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	offset, exists := kp.GroupCommittedOffsets[group]
	if !exists {
		kp.GroupCommittedOffsets[group] = 0
	}
	return offset
}
