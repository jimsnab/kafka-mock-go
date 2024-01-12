package kafkamock

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/google/uuid"
)

type (
	kafkaResponse struct {
		packet bytes.Buffer
		tags   map[int]any
	}

	VarInt                int32
	VarUint               uint32
	VarInt64              int64
	VarUint64             uint64
	CompactString         string
	NullableString        *string
	CompactNullableString *CompactString
	CompactBytes          []byte
	NullableBytes         []byte
	CompactNullableBytes  []byte
	CompactArray          any
)

func newKafkaResponse() *kafkaResponse {
	buffer := make([]byte, 0, 8192)
	return &kafkaResponse{
		packet: *bytes.NewBuffer(buffer),
	}
}

func (kr *kafkaResponse) writeBool(v bool) {
	if v {
		kr.packet.WriteByte(1)
	} else {
		kr.packet.WriteByte(0)
	}
}

func (kr *kafkaResponse) writeInt8(v int8) {
	kr.packet.WriteByte(byte(v))
}

func (kr *kafkaResponse) writeInt16(v int16) {
	bytes := []byte{0, 0}
	binary.BigEndian.PutUint16(bytes, uint16(v))
	kr.packet.Write(bytes)
}

func (kr *kafkaResponse) writeInt32(v int32) {
	bytes := []byte{0, 0, 0, 0}
	convertInt32(bytes, v)
	kr.packet.Write(bytes)
}

func (kr *kafkaResponse) writeInt64(v int64) {
	bytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(bytes, uint64(v))
	kr.packet.Write(bytes)
}

func (kr *kafkaResponse) writeUint32(v uint32) {
	bytes := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(bytes, v)
	kr.packet.Write(bytes)
}

func (kr *kafkaResponse) writeVarInt(v VarInt) {
	bytes := makeZigzag32(v)
	kr.packet.Write(bytes)
}

func (kr *kafkaResponse) writeVarInt64(v VarInt64) {
	bytes := makeZigzag64(v)
	kr.packet.Write(bytes)
}

func (kr *kafkaResponse) writeUuid(v uuid.UUID) {
	bytes, _ := v.MarshalBinary()
	kr.packet.Write(bytes)
}

func (kr *kafkaResponse) writeFloat64(v float64) {
	bytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(bytes, math.Float64bits(v))
	kr.packet.Write(bytes)
}

func (kr *kafkaResponse) writeString(v string) {
	kr.writeNullableString(&v)
}

func (kr *kafkaResponse) writeCompactString(v CompactString) {
	kr.writeCompactNullableString(&v)
}

func (kr *kafkaResponse) writeNullableString(v *string) {
	if v == nil {
		kr.writeInt16(0)
	} else {
		kr.writeInt16(int16(len(*v)))
		kr.packet.Write([]byte(*v))
	}
}

func (kr *kafkaResponse) writeCompactNullableString(v CompactNullableString) {
	if v == nil {
		kr.writeVarInt(0)
	} else {
		kr.writeVarInt(VarInt(len(*v)))
		kr.packet.Write([]byte(*v))
	}
}

func (kr *kafkaResponse) writeBytes(v []byte) {
	kr.writeNullableBytes(v)
}

func (kr *kafkaResponse) writeCompactBytes(v []byte) {
	kr.writeCompactNullableBytes(v)
}

func (kr *kafkaResponse) writeNullableBytes(v NullableBytes) {
	kr.writeUint32(uint32(len(v)))
	kr.packet.Write(v)
}

func (kr *kafkaResponse) writeCompactNullableBytes(v CompactNullableBytes) {
	if v == nil {
		kr.writeVarInt(0)
	} else {
		kr.writeVarInt(VarInt(len(v) + 1))
		kr.packet.Write(v)
	}
}

func (kr *kafkaResponse) writeArray(v []any) {
	if v == nil {
		kr.writeInt32(0)
	} else {
		kr.writeInt32(int32(len(v)) + 1)
		for _, vi := range v {
			kr.writeValue(vi)
		}
	}
}

func (kr *kafkaResponse) writeCompactArray(v CompactArray) {
	panic("obsolete")
}

func (kr *kafkaResponse) writeValue(v any) {
	switch t := v.(type) {
	case int8:
		kr.writeInt8(t)
	case int16:
		kr.writeInt16(t)
	case int32:
		kr.writeInt32(t)
	case int64:
		kr.writeInt64(t)
	case uint32:
		kr.writeUint32(t)
	case VarInt:
		kr.writeVarInt(t)
	case VarInt64:
		kr.writeVarInt64(t)
	case uuid.UUID:
		kr.writeUuid(t)
	case float64:
		kr.writeFloat64(t)
	case string:
		kr.writeString(t)
	case *string:
		kr.writeNullableString(t)
	case NullableString:
		kr.writeNullableString(t)
	case CompactString:
		kr.writeCompactString(t)
	case CompactNullableString:
		kr.writeCompactNullableString(t)
	case []byte:
		kr.writeBytes(t)
	case CompactBytes:
		kr.writeCompactBytes(t)
	case NullableBytes:
		kr.writeNullableBytes(t)
	case CompactNullableBytes:
		kr.writeCompactNullableBytes(t)
	case []any:
		kr.writeArray(t)
	default:
		panic(fmt.Sprintf("unsupported type %T", t))
	}
}

func (kr *kafkaResponse) writeTags(v map[int]any) {
	if len(v) == 0 {
		kr.writeInt8(0)
	} else {
		ids := make([]int, 0, len(v))
		for id := range v {
			ids = append(ids, id)
		}
		sort.Ints(ids)

		kr.writeVarInt(VarInt(len(ids)))
		for _, id := range ids {
			kr.writeVarInt(VarInt(id)) // field tag

			// inefficient way to serialize the tag value, but it's infrequently used
			vi := v[id]
			kr2 := newKafkaResponse()
			kr2.writeValue(vi)

			kr.writeVarInt(VarInt(kr2.packet.Len())) // field length
			kr.writeBytes(kr2.packet.Bytes())        // field data
		}
	}
}
