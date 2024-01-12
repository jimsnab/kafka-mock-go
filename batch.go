package kafkamock

import (
	"bufio"
	"bytes"
	"hash/crc32"
)

type (
	messageSetV1 struct {
		Offset int64
		Messages []byte
		msgsize int
	}

	messageV1 struct {
		Crc int32
		MagicByte int8
		Attributes int8
		Timestamp int64
		Key []byte
		Value []byte
	}
)

var crcTable *crc32.Table = crc32.MakeTable(crc32.Castagnoli)

func newMessageSetV1(offset int64) *messageSetV1 {
	return &messageSetV1{
		Offset: offset,
		msgsize: 12,
	}
}

func (msv1 *messageSetV1) appendMessage(record *kafkaRecord, maxSize int) bool {
	mv1 := messageV1{
		MagicByte: 1,
		Attributes: 0,
		Timestamp: record.Timestamp,
		Key: record.Key,
		Value: record.Value,
	}
	newSize := int(msv1.msgsize) + 18 + len(mv1.Key) + len(mv1.Value)
	if newSize > maxSize {
		return false
	}
	
	mv1.Crc = int32(mv1.crc())

	data := encodeMessage(mv1)
	msv1.Messages = append(msv1.Messages, data...)
	msv1.msgsize += len(data)
	return true
}

func (mv1 *messageV1) crc() uint32 {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	encodeObject(w, mv1)
	w.Flush()

	return crc32.Update(0, crcTable, buf.Bytes()[4:])
}
