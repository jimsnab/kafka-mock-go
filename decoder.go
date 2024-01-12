package kafkamock

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/google/uuid"
)

// worker to read the variable-length data that has a 16-bit length
func peekNullableData16(reader *bufio.Reader, offset int) (next int, data []byte) {
	next = offset + 2
	data, unfilled := reader.Peek(next)
	if unfilled != nil {
		next = -1
		return
	}

	count := int(int16(binary.BigEndian.Uint16(data[offset:])))
	if count < 0 {
		data = nil
		return
	}

	start := next
	next += count
	data, unfilled = reader.Peek(next)
	if unfilled != nil {
		next = -1
		return
	}

	data = data[start:]
	return
}

func peekVarUint(reader *bufio.Reader, offset int) (next int, varuint VarUint) {
	next = offset
	inbound, _ := reader.Peek(offset + 5)
	n := len(inbound)
	shift := 0
	for {
		if next >= n {
			next = -1
			return
		}
		by := inbound[next]
		next++

		varuint = varuint | (VarUint(by&0x7F) << shift)
		if (by & 0x80) == 0 {
			return
		}
		shift += 7
	}
}

func peekVarInt(reader *bufio.Reader, offset int) (next int, varint VarInt) {
	next, varuint := peekVarUint(reader, offset)
	if next < 0 {
		return
	}

	varint = VarInt((varuint >> 1) ^ (-(varuint & 1)))
	return
}

func peekCompactNullableBytes(reader *bufio.Reader, offset int) (next int, data CompactNullableBytes) {
	next, count := peekVarInt(reader, offset)
	if next < 0 {
		return
	}

	if count <= 0 {
		return
	}

	count--
	inbound, unfilled := reader.Peek(next + int(count))
	if unfilled != nil {
		next = -1
		return
	}

	data = inbound[next:]
	next += int(count)
	return
}

func peekVarInt64(reader *bufio.Reader, offset int) (next int, varint VarInt64) {
	next, varuint := peekVarUint64(reader, offset)
	if next < 0 {
		return
	}
	varint = VarInt64((varuint >> 1) ^ (-(varuint & 1)))
	return
}

func peekVarUint64(reader *bufio.Reader, offset int) (next int, varuint VarUint64) {
	next = offset
	inbound, _ := reader.Peek(offset + 10)
	n := len(inbound)
	shift := 0
	for {
		if next >= n {
			next = -1
			return
		}
		by := inbound[next]
		next++

		varuint = varuint | (VarUint64(by&0x7F) << shift)
		if (by & 0x80) == 0 {
			return
		}
		shift += 7
	}
}

func peekNullableString(reader *bufio.Reader, offset int) (next int, str NullableString) {
	next, data := peekNullableData16(reader, offset)
	if next < 0 {
		return
	}

	if data == nil {
		return
	}

	s := string(data)
	str = &s
	return
}

func peekCompactNullableString(reader *bufio.Reader, offset int) (next int, str CompactNullableString) {
	next, data := peekCompactNullableBytes(reader, offset)
	if next < 0 {
		return
	}

	if data == nil {
		return
	}

	s := CompactString(data)
	str = &s
	return
}

func peekString(reader *bufio.Reader, offset int) (next int, str string) {
	next, s := peekObject(reader, offset, reflect.TypeOf(str))
	if next < 0 {
		return
	}

	str = s.(string)
	return
}

func peekNullableBytes(reader *bufio.Reader, offset int) (next int, data NullableBytes) {
	next, length := peekInt32(reader, offset)
	if next < 0 {
		return
	}

	if length < 0 {
		return
	}

	start := next
	next += int(length)
	data, unfilled := reader.Peek(next)
	if unfilled != nil {
		next = -1
		return
	}

	data = data[start:]
	return
}

func peekTags(reader *bufio.Reader, offset int, fields map[int]*kafkaField) (next int, tags map[int]any) {
	t := map[int]any{}

	next, values := peekVarUint(reader, offset)
	if next < 0 {
		return
	}

	for valNum := uint(0); valNum < uint(values); valNum++ {
		var tag VarUint
		next, tag = peekVarUint(reader, next)
		if next < 0 {
			return
		}

		var taglen VarUint
		next, taglen = peekVarUint(reader, next)
		if next < 0 {
			return
		}
		tl := int(taglen)

		f, known := fields[int(tag)]
		if !known {
			inbound, unfilled := reader.Peek(next + tl)
			if unfilled != nil {
				next = -1
				return
			}
			t[int(tag)] = inbound[next:]
			next += tl
		} else {
			var item any
			next, item = peekObject(reader, next, f.valType)
			if next < 0 {
				return
			}
			t[int(tag)] = item
		}
	}

	tags = t
	return
}

func peekFixedData(reader *bufio.Reader, offset int, length int) (next int, data []byte) {
	next = offset + length
	inbound, unfilled := reader.Peek(next)
	if unfilled != nil {
		next = -1
		return
	}
	data = inbound[offset:]
	return
}

func peekBool(reader *bufio.Reader, offset int) (next int, v bool) {
	next, data := peekFixedData(reader, offset, 1)
	if next < 0 {
		return
	}

	if data[0] != 0 {
		v = true
	}
	return
}

func peekInt8(reader *bufio.Reader, offset int) (next int, v int8) {
	next, data := peekFixedData(reader, offset, 1)
	if next < 0 {
		return
	}

	v = int8(data[0])
	return
}

func peekUint8(reader *bufio.Reader, offset int) (next int, v uint8) {
	next, data := peekFixedData(reader, offset, 1)
	if next < 0 {
		return
	}

	v = uint8(data[0])
	return
}

func peekInt16(reader *bufio.Reader, offset int) (next int, v int16) {
	next, data := peekFixedData(reader, offset, 2)
	if next < 0 {
		return
	}

	v = int16(binary.BigEndian.Uint16(data))
	return
}

func peekUint16(reader *bufio.Reader, offset int) (next int, v uint16) {
	next, data := peekFixedData(reader, offset, 2)
	if next < 0 {
		return
	}

	v = binary.BigEndian.Uint16(data)
	return
}

func peekInt32(reader *bufio.Reader, offset int) (next int, v int32) {
	next, data := peekFixedData(reader, offset, 4)
	if next < 0 {
		return
	}

	v = int32(binary.BigEndian.Uint32(data))
	return
}

func peekUint32(reader *bufio.Reader, offset int) (next int, v uint32) {
	next, data := peekFixedData(reader, offset, 4)
	if next < 0 {
		return
	}

	v = binary.BigEndian.Uint32(data)
	return
}

func peekInt64(reader *bufio.Reader, offset int) (next int, v int64) {
	next, data := peekFixedData(reader, offset, 8)
	if next < 0 {
		return
	}

	v = int64(binary.BigEndian.Uint64(data))
	return
}

func peekUint64(reader *bufio.Reader, offset int) (next int, v uint64) {
	next, data := peekFixedData(reader, offset, 8)
	if next < 0 {
		return
	}

	v = binary.BigEndian.Uint64(data)
	return
}

func peekFloat64(reader *bufio.Reader, offset int) (next int, f float64) {
	next, data := peekFixedData(reader, offset, 8)
	if next < 0 {
		return
	}

	u := binary.BigEndian.Uint64(data)
	f = math.Float64frombits(u)
	return
}

func peekObject(reader *bufio.Reader, offset int, tt reflect.Type) (next int, obj any) {
	return peekObjectWorker(reader, offset, tt, false, false)
}

func peekObjectWorker(reader *bufio.Reader, offset int, tt reflect.Type, nullable, compact bool) (next int, obj any) {
	switch tt.Kind() {
	case reflect.Bool:
		return peekBool(reader, offset)

	case reflect.Int8:
		return peekInt8(reader, offset)

	case reflect.Int16:
		return peekInt16(reader, offset)

	case reflect.Int32:
		if tt.Name() == "VarInt" {
			return peekVarInt(reader, offset)
		} else {
			return peekInt32(reader, offset)
		}

	case reflect.Int:
		var v int32
		next, v = peekInt32(reader, offset)
		obj = int(v)
		return

	case reflect.Int64:
		if tt.Name() == "VarInt64" {
			return peekVarInt64(reader, offset)
		} else {
			return peekInt64(reader, offset)
		}

	case reflect.Uint:
		var v uint32
		next, v = peekUint32(reader, offset)
		obj = uint(v)
		return

	case reflect.Uint8:
		return peekUint8(reader, offset)

	case reflect.Uint16:
		return peekUint16(reader, offset)

	case reflect.Uint32:
		if tt.Name() == "VarUint" {
			return peekVarUint(reader, offset)
		} else {
			return peekUint32(reader, offset)
		}

	case reflect.Uint64:
		if tt.Name() == "VarUint64" {
			return peekVarUint64(reader, offset)
		} else {
			return peekUint64(reader, offset)
		}

	case reflect.Float64:
		return peekFloat64(reader, offset)

	case reflect.Array:
		if tt.Name() == "UUID" {
			var bytes []byte
			next, bytes = peekFixedData(reader, offset, 16)
			if next < 0 {
				return
			}
			var invalid error
			obj, invalid = uuid.FromBytes(bytes)
			if invalid != nil {
				panic(invalid)
			}
			return
		} else {
			// This is a trick to be able to distinguish kafka's normal arrays from its compact arrays.
			// Use a fixed array type for a compact array, and a slice type for a normal array.
			return peekObjectCompactVarArray(reader, offset, tt.Elem())
		}

	case reflect.Slice:
		if tt.Name() == "NullableBytes" {
			return peekNullableBytes(reader, offset)
		} else if tt.Name() == "CompactNullableBytes" {
			return peekCompactNullableBytes(reader, offset)
		} else {
			if compact {
				return peekObjectCompactVarArray(reader, offset, tt.Elem())
			} else {
				return peekObjectVarArray(reader, offset, tt.Elem())
			}
		}

	case reflect.String:
		if tt.Name() == "CompactString" {
			var s CompactNullableString
			next, s = peekCompactNullableString(reader, offset)
			if next < 0 {
				return
			}
			if s == nil {
				obj = CompactString("")
			} else {
				obj = *s
			}
		} else {
			var s NullableString
			next, s = peekNullableString(reader, offset)
			if next < 0 {
				return
			}
			if s == nil {
				obj = ""
			} else {
				obj = *s
			}
		}
		return

	case reflect.Struct:
		return peekObjectStruct(reader, offset, tt)

	case reflect.Pointer:
		switch tt.Name() {
		case "CompactNullableString":
			return peekCompactNullableString(reader, offset)
		case "NullableString":
			return peekNullableString(reader, offset)
		default:
			// pointer to something
			elem := tt.Elem()

			if elem.Kind() == reflect.Struct {
				var v any
				next, v = peekObject(reader, offset, elem)
				if next < 0 {
					return
				}
				pt := reflect.PointerTo(elem)
				pv := reflect.New(pt.Elem())
				pv.Elem().Set(reflect.ValueOf(v))
				obj = pv.Interface()
				return
			}

			panic(fmt.Sprintf("unsupported object type %s", tt.Name()))
		}

	default:
		panic(fmt.Sprintf("unsupported reflect object kind %d", tt.Kind()))
	}
}

func kafkaTags(ft reflect.StructField) (nullable, compact bool) {
	tag := ft.Tag.Get("kafka")
	if tag != "" {
		parts := strings.Split(tag, ",")
		for _, part := range parts {
			if part == "compact" {
				compact = true
			} else if part == "nullable" {
				nullable = true
			}
		}
	}
	return
}

func peekObjectFixedArray(reader *bufio.Reader, offset, length int, elem reflect.Type) (next int, obj any) {
	next = offset
	if length >= 0 {
		a := reflect.MakeSlice(reflect.SliceOf(elem), 0, length)
		for i := 0; i < int(length); i++ {
			var item any
			next, item = peekObject(reader, next, elem)
			if next < 0 {
				return
			}
			a = reflect.Append(a, reflect.ValueOf(item))
		}
		obj = a.Interface()
	}

	return
}

func peekObjectVarArray(reader *bufio.Reader, offset int, elem reflect.Type) (next int, obj any) {
	next, length := peekInt32(reader, offset)
	if next < 0 {
		return
	}

	return peekObjectFixedArray(reader, next, int(length), elem)
}

func peekObjectCompactVarArray(reader *bufio.Reader, offset int, elem reflect.Type) (next int, obj any) {
	next, length := peekVarInt(reader, offset)
	if next < 0 {
		return
	}

	if length == 0 {
		return
	}

	return peekObjectFixedArray(reader, next, int(length-1), elem)
}

func peekObjectStruct(reader *bufio.Reader, offset int, tt reflect.Type) (next int, obj any) {
	next = offset
	o := reflect.New(tt)
	for i := 0; i < tt.NumField(); i++ {
		ft := tt.Field(i)

		nullable, compact := kafkaTags(ft)

		var v any
		next, v = peekObjectWorker(reader, next, ft.Type, nullable, compact)
		if next < 0 {
			return
		}
		f := o.Elem().Field(i)
		f.Set(reflect.ValueOf(v))
	}
	obj = o.Elem().Interface()
	return
}
