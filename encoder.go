package kafkamock

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"reflect"
	"sort"

	"github.com/google/uuid"
)

type (
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

	messageSetV1 struct {
		msgs      []messageV1
		offset    int64
		totalSize int
	}

	messageV1 struct {
		Offset      int64
		MessageSize int32
		Crc         int32
		MagicByte   int8
		Attributes  int8
		Timestamp   int64
		Key         []byte
		Value       []byte
	}
)

func encodeObject(writer *bufio.Writer, obj any) {
	encodeObjectWorker(writer, reflect.TypeOf(obj), obj, false, false)
}

func encodeObjectWorker(writer *bufio.Writer, tt reflect.Type, obj any, nullable, compact bool) {
	switch tt.Kind() {
	case reflect.Bool:
		encodeBool(writer, obj.(bool))
	case reflect.Int8:
		encodeInt8(writer, obj.(int8))
	case reflect.Int16:
		encodeInt16(writer, obj.(int16))
	case reflect.Int32:
		if tt.Name() == "VarInt" {
			encodeVarInt(writer, obj.(VarInt))
		} else if compact {
			encodeVarInt(writer, VarInt(obj.(int32)))
		} else {
			encodeInt32(writer, obj.(int32))
		}
	case reflect.Int:
		if compact {
			encodeVarInt(writer, VarInt(obj.(int)))
		} else {
			encodeInt32(writer, int32(obj.(int)))
		}
	case reflect.Int64:
		if tt.Name() == "VarInt64" {
			encodeVarInt64(writer, obj.(VarInt64))
		} else if compact {
			encodeVarInt64(writer, VarInt64(obj.(int64)))
		} else {
			encodeInt64(writer, obj.(int64))
		}
	case reflect.Uint8:
		encodeUint8(writer, obj.(uint8))
	case reflect.Uint16:
		encodeUint16(writer, obj.(uint16))
	case reflect.Uint32:
		if tt.Name() == "VarUint" {
			encodeVarUint(writer, obj.(VarUint))
		} else if compact {
			encodeVarUint(writer, VarUint(obj.(uint32)))
		} else {
			encodeUint32(writer, obj.(uint32))
		}
	case reflect.Uint:
		if compact {
			encodeVarUint(writer, VarUint(obj.(uint)))
		} else {
			encodeUint32(writer, uint32(obj.(uint)))
		}
	case reflect.Uint64:
		if tt.Name() == "VarUint64" {
			encodeVarUint64(writer, obj.(VarUint64))
		} else if compact {
			encodeVarUint64(writer, VarUint64(obj.(uint64)))
		} else {
			encodeUint64(writer, obj.(uint64))
		}
	case reflect.Float64:
		encodeFloat64(writer, obj.(float64))
	case reflect.String:
		if tt.Name() == "CompactString" {
			encodeCompactString(writer, obj.(CompactString))
		} else if compact {
			if nullable {
				if obj == nil {
					encodeCompactNullableString(writer, nil)
				} else {
					s := obj.(*string)
					s2 := CompactString(*s)
					encodeCompactNullableString(writer, &s2)
				}
			} else {
				encodeCompactString(writer, CompactString(obj.(string)))
			}
		} else {
			if nullable {
				if obj == nil {
					encodeNullableString(writer, nil)
				} else {
					s := obj.(string)
					encodeNullableString(writer, NullableString(&s))
				}
			} else {
				encodeString(writer, obj.(string))
			}
		}
	case reflect.Struct:
		if tt.Name() == "messageSetV1" {
			msv1 := obj.(messageSetV1)
			encodeMessageSetV1(writer, &msv1)
		} else {
			encodeStruct(writer, reflect.ValueOf(obj))
		}
	case reflect.Slice:
		et := tt.Elem()
		if et.Kind() == reflect.Uint8 {
			// byte slice
			if compact {
				if nullable {
					encodeCompactNullableBytes(writer, obj.([]byte))
				} else {
					encodeCompactBytes(writer, obj.([]byte))
				}
			} else {
				if nullable {
					encodeNullableBytes(writer, obj.([]byte))
				} else if tt.Name() == "NullableBytes" {
					encodeNullableBytes(writer, obj.(NullableBytes))
				} else if tt.Name() == "CompactNullableBytes" {
					encodeCompactNullableBytes(writer, obj.(CompactNullableBytes))
				} else if tt.Name() == "CompactBytes" {
					encodeCompactBytes(writer, obj.(CompactBytes))
				} else {
					encodeBytes(writer, obj.([]byte))
				}
			}
		} else {
			// object slice
			if compact {
				encodeCompactArray(writer, reflect.ValueOf(obj))
			} else {
				encodeArray(writer, reflect.ValueOf(obj))
			}
		}
	case reflect.Pointer:
		if tt.Name() == "CompactNullableString" {
			encodeCompactNullableString(writer, obj.(CompactNullableString))
		} else if tt.Name() == "NullableString" {
			encodeNullableString(writer, obj.(NullableString))
		} else if tt.Name() == "messageSetV1" {
			// message sets deviate from the normal wire data protocol
			encodeMessageSetV1(writer, obj.(*messageSetV1))
		} else {
			v := reflect.ValueOf(obj)
			if v.Kind() > 0 {
				if v.IsNil() {
					encodeObjectWorker(writer, targetType(obj), nil, true, compact)
				} else {
					target := reflect.Indirect(v)
					encodeObjectWorker(writer, target.Type(), target.Interface(), true, compact)
				}
			} else {
				panic(fmt.Sprintf("unsupported pointer type %T", obj))
			}
		}
	default:
		switch tt.Name() {
		case "UUID":
			encodeUuid(writer, obj.(uuid.UUID))
		default:
			panic(fmt.Sprintf("encoding unsupported for %T", obj))
		}
	}
}

func targetType(obj any) reflect.Type {
	switch obj.(type) {
	case *string:
		var zero string
		return reflect.TypeOf(zero)
	}

	tt := reflect.TypeOf(obj)

	switch tt.Kind() {
	case reflect.Struct:
		return tt

	case reflect.Pointer:
		v := reflect.ValueOf(obj)
		if v.IsNil() {
			return v.Type()
		}
		// pointer to a struct is supported the same as the struct itself
		v = reflect.Indirect(v)
		if v.Type().Kind() == reflect.Struct {
			return tt
		}
	}

	panic(fmt.Sprintf("unsupported target type %T", obj))
}

func encodeStruct(writer *bufio.Writer, v reflect.Value) {
	for i := 0; i < v.NumField(); i++ {
		tt := v.Type()
		tf := tt.Field(i)

		nullable, compact := kafkaTags(tf)

		f := v.Field(i)
		if f.CanInterface() {
			encodeObjectWorker(writer, f.Type(), f.Interface(), nullable, compact)
		}
	}
}

func encodeBool(writer *bufio.Writer, v bool) {
	if v {
		writer.WriteByte(1)
	} else {
		writer.WriteByte(0)
	}
}

func encodeInt8(writer *bufio.Writer, v int8) {
	writer.WriteByte(byte(v))
}

func encodeInt16(writer *bufio.Writer, v int16) {
	bytes := []byte{0, 0}
	binary.BigEndian.PutUint16(bytes, uint16(v))
	writer.Write(bytes)
}

func encodeInt32(writer *bufio.Writer, v int32) {
	bytes := []byte{0, 0, 0, 0}
	convertInt32(bytes, v)
	writer.Write(bytes)
}

func encodeInt64(writer *bufio.Writer, v int64) {
	bytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(bytes, uint64(v))
	writer.Write(bytes)
}

func encodeUint8(writer *bufio.Writer, v uint8) {
	writer.WriteByte(byte(v))
}

func encodeUint16(writer *bufio.Writer, v uint16) {
	bytes := []byte{0, 0}
	binary.BigEndian.PutUint16(bytes, v)
	writer.Write(bytes)
}

func encodeUint32(writer *bufio.Writer, v uint32) {
	bytes := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(bytes, v)
	writer.Write(bytes)
}

func encodeUint64(writer *bufio.Writer, v uint64) {
	bytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(bytes, v)
	writer.Write(bytes)
}

func convertInt32(bytes []byte, v int32) {
	binary.BigEndian.PutUint32(bytes, uint32(v))
}

func makeZigzag32(n VarInt) []byte {
	n = (n << 1) ^ (n >> 31)
	return makeVarUint(uint32(n))
}

func makeVarUint(v uint32) []byte {
	vi := make([]byte, 0, 5)
	for {
		by := byte(v & 0x7F)
		if v > 0x7F {
			vi = append(vi, by|0x80)
			v = v >> 7
		} else {
			vi = append(vi, by)
			break
		}
	}

	return vi
}

func makeZigzag64(n VarInt64) []byte {
	n = (n << 1) ^ (n >> 63)
	return makeVarUint64(uint64(n))
}

func makeVarUint64(v uint64) []byte {
	vi := make([]byte, 0, 10)
	for {
		by := byte(v & 0x7F)
		if v > 0x7F {
			vi = append(vi, by|0x80)
			v = v >> 7
		} else {
			vi = append(vi, by)
			break
		}
	}

	return vi
}

func encodeVarInt(writer *bufio.Writer, v VarInt) {
	bytes := makeZigzag32(v)
	writer.Write(bytes)
}

func encodeVarUint(writer *bufio.Writer, v VarUint) {
	bytes := makeVarUint(uint32(v))
	writer.Write(bytes)
}

func encodeVarInt64(writer *bufio.Writer, v VarInt64) {
	bytes := makeZigzag64(v)
	writer.Write(bytes)
}

func encodeVarUint64(writer *bufio.Writer, v VarUint64) {
	bytes := makeVarUint64(uint64(v))
	writer.Write(bytes)
}

func encodeUuid(writer *bufio.Writer, v uuid.UUID) {
	bytes, _ := v.MarshalBinary()
	writer.Write(bytes)
}

func encodeFloat64(writer *bufio.Writer, v float64) {
	bytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(bytes, math.Float64bits(v))
	writer.Write(bytes)
}

func encodeString(writer *bufio.Writer, v string) {
	encodeNullableString(writer, &v)
}

func encodeCompactString(writer *bufio.Writer, v CompactString) {
	encodeCompactNullableString(writer, &v)
}

func encodeNullableString(writer *bufio.Writer, v NullableString) {
	if v == nil {
		encodeInt16(writer, -1)
	} else {
		encodeInt16(writer, int16(len(*v)))
		writer.Write([]byte(*v))
	}
}

func encodeCompactNullableString(writer *bufio.Writer, v CompactNullableString) {
	if v == nil {
		encodeVarInt(writer, 0)
	} else {
		encodeVarInt(writer, VarInt(len(*v)+1))
		writer.Write([]byte(*v))
	}
}

func encodeBytes(writer *bufio.Writer, v []byte) {
	encodeNullableBytes(writer, v)
}

func encodeCompactBytes(writer *bufio.Writer, v []byte) {
	encodeCompactNullableBytes(writer, v)
}

func encodeNullableBytes(writer *bufio.Writer, v NullableBytes) {
	if reflect.ValueOf(v).IsNil() {
		encodeInt32(writer, -1)
	} else {
		encodeUint32(writer, uint32(len(v)))
		writer.Write(v)
	}
}

func encodeCompactNullableBytes(writer *bufio.Writer, v CompactNullableBytes) {
	if v == nil {
		encodeVarInt(writer, 0)
	} else {
		encodeVarInt(writer, VarInt(len(v)+1))
		writer.Write(v)
	}
}

func encodeArray(writer *bufio.Writer, v reflect.Value) {
	if v.IsNil() {
		encodeInt32(writer, -1)
	} else {
		encodeInt32(writer, int32(v.Len()))

		for i := 0; i < v.Len(); i++ {
			f := v.Index(i)
			encodeObject(writer, f.Interface())
		}
	}
}

func encodeCompactArray(writer *bufio.Writer, v reflect.Value) {
	if v.IsNil() {
		encodeVarInt(writer, 0)
	} else {
		encodeVarInt(writer, VarInt(v.Len())+1)

		for i := 0; i < v.Len(); i++ {
			f := v.Index(i)
			encodeObject(writer, f.Interface())
		}
	}
}

func encodeTags(writer *bufio.Writer, tags map[int]any) {
	encodeVarUint(writer, VarUint(len(tags)))

	ids := make([]int, 0, len(tags))
	for id := range tags {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	for _, id := range ids {
		tag := tags[id]
		encodeVarUint(writer, VarUint(id))

		var buf bytes.Buffer
		w := bufio.NewWriter(&buf)
		encodeObject(w, tag)
		w.Flush()

		encodeVarUint(writer, VarUint(buf.Len()))
		writer.Write(buf.Bytes())
	}
}

func encodeMessageSetV1(writer *bufio.Writer, ms *messageSetV1) {
	encodeObject(writer, ms.totalSize)
	for _, msg := range ms.msgs {
		encodeObject(writer, msg)
	}
}

var crcTable *crc32.Table = crc32.MakeTable(crc32.Castagnoli)

func newMessageSetV1(offset int64) *messageSetV1 {
	return &messageSetV1{
		offset: offset,
		msgs:   []messageV1{},
	}
}

func (msv1 *messageSetV1) appendMessage(record *kafkaRecord, maxSize int) bool {
	mv1 := messageV1{
		Offset:     msv1.offset,
		MagicByte:  1,
		Attributes: 0,
		Timestamp:  record.Timestamp,
		Key:        record.Key,
		Value:      record.Value,
	}
	mv1.MessageSize = int32(34 + len(record.Key) + len(record.Value))
	mv1.Crc = int32(mv1.crc())

	newSize := msv1.totalSize + int(mv1.MessageSize)
	if newSize > maxSize {
		return false
	}

	msv1.totalSize += int(mv1.MessageSize)
	msv1.offset++

	msv1.msgs = append(msv1.msgs, mv1)
	return true
}

func (mv1 *messageV1) crc() uint32 {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	encodeObject(w, mv1)
	w.Flush()

	return crc32.Update(0, crcTable, buf.Bytes()[16:])
}
