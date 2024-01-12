package kafkamock

import (
	"bufio"
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

type (
	testStruct struct {
		Value1 int
		Value2 string
		Value3 []int16
		Value4 testValue
		Value5 *testValue
		Value6 []int16 `kafka:"compact"`
	}

	testValue struct {
		A uint
		B uint
	}

	compactInt32 struct {
		A int32 `kafka:"compact"`
	}

	compactUint32 struct {
		A uint32 `kafka:"compact"`
	}

	compactInt64 struct {
		A int64 `kafka:"compact"`
	}

	compactUint64 struct {
		A uint64 `kafka:"compact"`
	}

	compactString struct {
		A string `kafka:"compact"`
	}

	compactNullableString struct {
		A *CompactString `kafka:"compact,nullable"`
	}

	compactInt32Array struct {
		A []int32 `kafka:"compact"`
	}

	nullableBytes struct {
		A []byte
	}

	compactNullableBytes struct {
		A []byte `kafka:"compact"`
	}
)

func TestEncodeDecodeBool(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, true)
	writer.Flush()

	var zero bool
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(bool)
	if !ok {
		t.Error("expected type")
	}
	if !n {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeInt8(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, int8(123))
	encodeObject(writer, int8(34))
	writer.Flush()

	var zero int8
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(int8)
	if !ok {
		t.Error("expected type")
	}
	if n != 123 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(int8)
	if !ok {
		t.Error("expected type")
	}
	if n != 34 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeInt16(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, int16(1234))
	encodeObject(writer, int16(12345))
	writer.Flush()

	var zero int16
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(int16)
	if !ok {
		t.Error("expected type")
	}
	if n != 1234 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(int16)
	if !ok {
		t.Error("expected type")
	}
	if n != 12345 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeInt32(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, int32(12345))
	encodeObject(writer, int32(123456))
	writer.Flush()

	var zero int32
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(int32)
	if !ok {
		t.Error("expected type")
	}
	if n != 12345 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(int32)
	if !ok {
		t.Error("expected type")
	}
	if n != 123456 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeInt64(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, int64(123456))
	encodeObject(writer, int64(math.MinInt64))
	writer.Flush()

	var zero int64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(int64)
	if !ok {
		t.Error("expected type")
	}
	if n != 123456 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(int64)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MinInt64 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeUint8(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, uint8(123))
	encodeObject(writer, uint8(250))
	writer.Flush()

	var zero uint8
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(uint8)
	if !ok {
		t.Error("expected type")
	}
	if n != 123 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(uint8)
	if !ok {
		t.Error("expected type")
	}
	if n != 250 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeUint16(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, uint16(1234))
	encodeObject(writer, uint16(math.MaxUint16))
	writer.Flush()

	var zero uint16
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(uint16)
	if !ok {
		t.Error("expected type")
	}
	if n != 1234 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(uint16)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxUint16 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeUint32(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, uint32(12345))
	encodeObject(writer, uint32(math.MaxUint32))
	writer.Flush()

	var zero uint32
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(uint32)
	if !ok {
		t.Error("expected type")
	}
	if n != 12345 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(uint32)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxUint32 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeUint64(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, uint64(123456))
	encodeObject(writer, uint64(math.MaxUint64))
	writer.Flush()

	var zero uint64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(uint64)
	if !ok {
		t.Error("expected type")
	}
	if n != 123456 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(uint64)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxUint64 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeFloat64(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, float64(123.456))
	encodeObject(writer, float64(math.MaxFloat64))
	writer.Flush()

	var zero float64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(float64)
	if !ok {
		t.Error("expected type")
	}
	if n != 123.456 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(float64)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxFloat64 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeString(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, "test")
	encodeObject(writer, "test2")
	writer.Flush()

	var zero string
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(string)
	if !ok {
		t.Error("expected type")
	}
	if n != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(string)
	if !ok {
		t.Error("expected type")
	}
	if n != "test2" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeStringUtil(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, "test")
	encodeObject(writer, "test2")
	writer.Flush()

	reader := bufio.NewReader(&buf)
	next, v := peekString(reader, 0)
	if next < 0 {
		t.Error("expected a value")
	}
	if v != "test" {
		t.Error("expected value match")
	}

	next, v = peekString(reader, next)
	if next < 0 {
		t.Error("expected a value")
	}
	if v != "test2" {
		t.Error("expected value match")
	}
}

func TestEncodeDecodeStringPartial(t *testing.T) {
	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	encodeObject(writer, "test")	
	writer.Flush()

	var buf bytes.Buffer
	writer = bufio.NewWriter(&buf)
	writer.Write(full.Bytes()[:full.Len() - 1])
	writer.Flush()

	reader := bufio.NewReader(&buf)
	next, _ := peekString(reader, 0)
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeNullableString(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	s := "test"	
	encodeObject(writer, &s)
	encodeObject(writer, &s)
	writer.Flush()

	var zero NullableString
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(NullableString)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || *n != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(NullableString)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || *n != "test" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeStringFromNullableString(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var s NullableString
	encodeObject(writer, s)
	writer.Flush()

	var zero string
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(string)
	if !ok {
		t.Error("expected type")
	}
	if n != "" {
		t.Error("expected value match")
	}
}

func TestEncodeDecodeNullableStringNil(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var s *string
	encodeObject(writer, s)
	writer.Flush()

	var zero NullableString
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(NullableString)
	if !ok {
		t.Error("expected type")
	}
	if n != nil {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarInt(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarInt(12))
	encodeObject(writer, VarInt(math.MaxInt32))
	writer.Flush()

	var zero VarInt
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarInt)
	if !ok {
		t.Error("expected type")
	}
	if n != 12 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(VarInt)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxInt32 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarIntTag(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, compactInt32{A: 12})
	encodeObject(writer, compactInt32{A: math.MaxInt32})
	writer.Flush()

	var zero VarInt
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarInt)
	if !ok {
		t.Error("expected type")
	}
	if n != 12 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(VarInt)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxInt32 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarInt2(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarInt(-1))
	writer.Flush()
	if buf.Len() != 1 {
		t.Error("-1 should take one byte")
	}

	var zero VarInt
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarInt)
	if !ok {
		t.Error("expected type")
	}
	if n != -1 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarInt3(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarInt(math.MinInt32))
	writer.Flush()
	if buf.Len() != 5 {
		t.Error("min int should take five bytes")
	}	

	var zero VarInt
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarInt)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MinInt32 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarInt64(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarInt64(12))
	encodeObject(writer, VarInt64(math.MaxInt64))
	writer.Flush()

	var zero VarInt64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarInt64)
	if !ok {
		t.Error("expected type")
	}
	if n != 12 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(VarInt64)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxInt64 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarInt64Tag(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, compactInt64{A: 12})
	encodeObject(writer, compactInt64{A: math.MaxInt64})
	writer.Flush()

	var zero VarInt64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarInt64)
	if !ok {
		t.Error("expected type")
	}
	if n != 12 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(VarInt64)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxInt64 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarInt642(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarInt64(-1))
	writer.Flush()
	if buf.Len() != 1 {
		t.Error("-1 should take one byte")
	}

	var zero VarInt64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarInt64)
	if !ok {
		t.Error("expected type")
	}
	if n != -1 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarInt643(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarInt64(math.MinInt64))
	writer.Flush()
	if buf.Len() != 10 {
		t.Error("min int 64 should take ten bytes")
	}	

	var zero VarInt64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarInt64)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MinInt64 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarUint(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarUint(1234))
	encodeObject(writer, VarUint(math.MaxUint32))
	writer.Flush()

	var zero VarUint
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarUint)
	if !ok {
		t.Error("expected type")
	}
	if n != 1234 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(VarUint)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxUint32 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarUintTag(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, compactUint32{A: 1234})
	encodeObject(writer, compactUint32{A: math.MaxUint32})
	writer.Flush()

	var zero VarUint
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarUint)
	if !ok {
		t.Error("expected type")
	}
	if n != 1234 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(VarUint)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxUint32 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarUint2(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarUint(1))
	writer.Flush()
	if buf.Len() != 1 {
		t.Error("1 should take one byte")
	}

	var zero VarUint
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarUint)
	if !ok {
		t.Error("expected type")
	}
	if n != 1 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarUint64(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarUint64(1234))
	encodeObject(writer, VarUint64(math.MaxUint64))
	writer.Flush()

	var zero VarUint64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarUint64)
	if !ok {
		t.Error("expected type")
	}
	if n != 1234 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(VarUint64)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxUint64 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarUint64Tag(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, compactUint64{A: 1234})
	encodeObject(writer, compactUint64{A: math.MaxUint64})
	writer.Flush()

	var zero VarUint64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarUint64)
	if !ok {
		t.Error("expected type")
	}
	if n != 1234 {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(VarUint64)
	if !ok {
		t.Error("expected type")
	}
	if n != math.MaxUint64 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeVarUint642(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, VarUint64(1))
	writer.Flush()
	if buf.Len() != 1 {
		t.Error("1 should take one byte")
	}

	var zero VarUint64
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(VarUint64)
	if !ok {
		t.Error("expected type")
	}
	if n != 1 {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeUuid(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, id1)
	encodeObject(writer, id2)
	writer.Flush()
	if buf.Len() != 32 {
		t.Error("expected 32 bytes")
	}

	var zero uuid.UUID
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(uuid.UUID)
	if !ok {
		t.Error("expected type")
	}
	if id1.String() != n.String() {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(uuid.UUID)
	if !ok {
		t.Error("expected type")
	}
	if id2.String() != n.String() {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactString(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, CompactString("test"))
	encodeObject(writer, CompactString("test2"))
	writer.Flush()

	var zero CompactString
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactString)
	if !ok {
		t.Error("expected type")
	}
	if n != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(CompactString)
	if !ok {
		t.Error("expected type")
	}
	if n != "test2" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactStringFromNullable(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var s CompactNullableString
	encodeObject(writer, s)
	writer.Flush()

	var zero CompactString
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactString)
	if !ok {
		t.Error("expected type")
	}
	if n != "" {
		t.Error("expected value match")
	}
}

func TestEncodeDecodeCompactStringTag(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, compactString{A:"test"})
	encodeObject(writer, compactString{A: "test2"})
	writer.Flush()

	var zero CompactString
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactString)
	if !ok {
		t.Error("expected type")
	}
	if n != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(CompactString)
	if !ok {
		t.Error("expected type")
	}
	if n != "test2" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactNullableString(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	s := CompactString("test")
	encodeObject(writer, CompactNullableString(&s))
	encodeObject(writer, CompactNullableString(&s))
	writer.Flush()

	var zero CompactNullableString
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactNullableString)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || *n != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(CompactNullableString)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || *n != "test" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactNullableStringPartial(t *testing.T) {
	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	s := CompactString("test")
	encodeObject(writer, CompactNullableString(&s))
	writer.Flush()

	for i := 1 ; i < full.Len() - 1 ; i++ {
		var buf bytes.Buffer
		writer = bufio.NewWriter(&buf)
		writer.Write(full.Bytes()[:i])
		writer.Flush()

		var zero CompactNullableString
		reader := bufio.NewReader(&buf)
		next, _ := peekObject(reader, 0, reflect.TypeOf(zero))
		if next >= 0 {
			t.Error("didn't expect a value")
		}
	}
}

func TestEncodeDecodeCompactNullableStringNilPartial(t *testing.T) {
	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	var s CompactNullableString
	encodeObject(writer, s)
	writer.Flush()

	for i := 1 ; i < full.Len() - 1 ; i++ {
		var buf bytes.Buffer
		writer = bufio.NewWriter(&buf)
		writer.Write(full.Bytes()[:i])
		writer.Flush()

		var zero CompactNullableString
		reader := bufio.NewReader(&buf)
		next, _ := peekObject(reader, 0, reflect.TypeOf(zero))
		if next >= 0 {
			t.Error("didn't expect a value")
		}
	}
}

func TestEncodeDecodeCompactNullableStringTag(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	s := CompactString("test")
	encodeObject(writer, compactNullableString{A: &s})
	encodeObject(writer, compactNullableString{A: &s})
	writer.Flush()

	var zero CompactNullableString
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactNullableString)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || *n != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(CompactNullableString)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || *n != "test" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactNullableStringNil(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var s CompactNullableString
	encodeObject(writer, s)
	writer.Flush()

	var zero CompactNullableString
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactNullableString)
	if !ok {
		t.Error("expected type")
	}
	if n != nil {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeNullableBytes(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	bytes := []byte("test")
	encodeObject(writer, NullableBytes(bytes))
	encodeObject(writer, NullableBytes(bytes))
	writer.Flush()

	var zero NullableBytes
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(NullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || string(n) != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(NullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || string(n) != "test" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeNullableBytesPartial(t *testing.T) {
	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	data := []byte("test")
	encodeObject(writer, NullableBytes(data))
	writer.Flush()

	for i := 1 ; i < full.Len() - 1 ; i++ {
		var buf bytes.Buffer
		writer = bufio.NewWriter(&buf)
		writer.Write(full.Bytes()[:i])
		writer.Flush()

		var zero NullableBytes
		reader := bufio.NewReader(&buf)
		next, _ := peekObject(reader, 0, reflect.TypeOf(zero))
		if next >= 0 {
			t.Error("didn't expect a value")
		}	
	}
}

func TestEncodeDecodeNullableBytesNilPartial(t *testing.T) {
	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	var data []byte
	encodeObject(writer, NullableBytes(data))
	writer.Flush()

	for i := 1 ; i < full.Len() - 1 ; i++ {
		var buf bytes.Buffer
		writer = bufio.NewWriter(&buf)
		writer.Write(full.Bytes()[:i])
		writer.Flush()

		var zero NullableBytes
		reader := bufio.NewReader(&buf)
		next, _ := peekObject(reader, 0, reflect.TypeOf(zero))
		if next >= 0 {
			t.Error("didn't expect a value")
		}	
	}
}

func TestEncodeDecodeNullableBytesTag(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	bytes := []byte("test")
	encodeObject(writer, nullableBytes{A: bytes})
	encodeObject(writer, nullableBytes{A: bytes})
	writer.Flush()

	var zero NullableBytes
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(NullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || string(n) != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(NullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || string(n) != "test" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeNullableBytesNil(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var bytes NullableBytes
	encodeObject(writer, bytes)
	writer.Flush()

	var zero NullableBytes
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(NullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n != nil {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactNullableBytes(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	bytes := []byte("test")
	encodeObject(writer, CompactNullableBytes(bytes))
	encodeObject(writer, CompactNullableBytes(bytes))
	writer.Flush()

	var zero CompactNullableBytes
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactNullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || string(n) != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(CompactNullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || string(n) != "test" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactNullableBytesPartial(t *testing.T) {
	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	data := []byte("test")
	encodeObject(writer, CompactNullableBytes(data))
	writer.Flush()

	for i := 1 ; i < full.Len() - 1 ; i++ {
		var buf bytes.Buffer
		writer = bufio.NewWriter(&buf)
		writer.Write(full.Bytes()[:i])
		writer.Flush()

		var zero CompactNullableBytes
		reader := bufio.NewReader(&buf)
		next, _ := peekObject(reader, 0, reflect.TypeOf(zero))
		if next >= 0 {
			t.Error("didn't expect a value")
		}	
	}
}

func TestEncodeDecodeCompactNullableBytesNilPartial(t *testing.T) {
	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	var data []byte
	encodeObject(writer, CompactNullableBytes(data))
	writer.Flush()

	for i := 1 ; i < full.Len() - 1 ; i++ {
		var buf bytes.Buffer
		writer = bufio.NewWriter(&buf)
		writer.Write(full.Bytes()[:i])
		writer.Flush()

		var zero CompactNullableBytes
		reader := bufio.NewReader(&buf)
		next, _ := peekObject(reader, 0, reflect.TypeOf(zero))
		if next >= 0 {
			t.Error("didn't expect a value")
		}	
	}
}

func TestEncodeDecodeCompactNullableBytesTag(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	bytes := []byte("test")
	encodeObject(writer, compactNullableBytes{A: bytes})
	encodeObject(writer, compactNullableBytes{A: bytes})
	writer.Flush()

	var zero CompactNullableBytes
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactNullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || string(n) != "test" {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok = v.(CompactNullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n == nil || string(n) != "test" {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactNullableBytesNil(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var bytes CompactNullableBytes
	encodeObject(writer, bytes)
	writer.Flush()

	var zero CompactNullableBytes
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	n, ok := v.(CompactNullableBytes)
	if !ok {
		t.Error("expected type")
	}
	if n != nil {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeArray(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	ref := []int32{1, 2, 3}
	encodeObject(writer, ref)
	encodeObject(writer, ref)
	writer.Flush()

	var zero []int32
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	a, ok := v.([]int32)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref, a) {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	a, ok = v.([]int32)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref, a) {
		t.Error("expected value match")
	}
	
	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeArrayNil(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var ref []int32
	encodeObject(writer, ref)
	writer.Flush()

	var zero []int32
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	if v != nil {
		t.Error("expected value match")
	}
}

func TestEncodeDecodeCompactArray(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	ref := compactInt32Array{A: []int32{1, 2, 3}}
	encodeObject(writer, ref)
	encodeObject(writer, ref)
	writer.Flush()

	var zero [0]int32
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	a, ok := v.([]int32)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref.A, a) {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	a, ok = v.([]int32)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref.A, a) {
		t.Error("expected value match")
	}
	
	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeCompactArrayNil(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var ref compactInt32Array
	encodeObject(writer, ref)
	writer.Flush()

	var zero [0]int32
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	if v != nil {
		t.Error("expected value match")
	}
}

func TestEncodeDecodeStruct(t *testing.T) {
	other := testValue{
		A: 222,
		B: 444,
	}
	ref := testStruct{
		Value1: 123,
		Value2: "test", 
		Value3: []int16{5, 10, 15},
		Value4: testValue{A: 4, B: 24},
		Value5: &other,
		Value6: []int16{100, 200},
	}

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, ref)
	encodeObject(writer, &ref)
	writer.Flush()

	var zero testStruct
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	s, ok := v.(testStruct)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref, s) {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	s, ok = v.(testStruct)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref, s) {
		t.Error("expected value match")
	}

	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeStructPartial(t *testing.T) {
	other := testValue{
		A: 222,
		B: 444,
	}
	ref := testStruct{
		Value1: 123,
		Value2: "test", 
		Value3: []int16{5, 10, 15},
		Value4: testValue{A: 4, B: 24},
		Value5: &other,
		Value6: []int16{100, 200},
	}

	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	encodeObject(writer, ref)
	writer.Flush()

	for i := 1 ; i < full.Len() - 1 ; i++ {
		var buf bytes.Buffer
		writer = bufio.NewWriter(&buf)
		writer.Write(full.Bytes()[:i])
		writer.Flush()

		var zero testStruct
		reader := bufio.NewReader(&buf)
		next, _ := peekObject(reader, 0, reflect.TypeOf(zero))
		if next >= 0 {
			t.Error("didn't expect a value")
		}
	}
}

func TestEncodeDecodeByteArray(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	ref := []byte{1, 2, 3}
	encodeObject(writer, ref)
	encodeObject(writer, ref)
	writer.Flush()

	var zero []byte
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	a, ok := v.([]byte)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref, a) {
		t.Error("expected value match")
	}

	next, v = peekObject(reader, next, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	a, ok = v.([]byte)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref, a) {
		t.Error("expected value match")
	}
	
	next, _ = peekObject(reader, next, reflect.TypeOf(zero))
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeByteArrayNil(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var ref []byte
	encodeObject(writer, ref)
	writer.Flush()

	var zero []byte
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	if v != nil {
		t.Error("expected value match")
	}
}

func TestEncodeDecodeTags(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	tags := map[int]any{
		1: "test",
		2: CompactString("test2"),
	}
	fields := map[int]*kafkaField{
		1: {valType: reflect.TypeOf(tags[1])},
		2: {valType: reflect.TypeOf(tags[2])},
	}
	encodeTags(writer, tags)
	encodeTags(writer, tags)
	writer.Flush()

	reader := bufio.NewReader(&buf)
	next, tg := peekTags(reader, 0, fields)
	if next < 0 {
		t.Error("expected a value")
	}
	if !reflect.DeepEqual(tags, tg) {
		t.Error("expected tag match")
	}

	next, tg = peekTags(reader, next, fields)
	if next < 0 {
		t.Error("expected a value")
	}
	if !reflect.DeepEqual(tags, tg) {
		t.Error("expected tag match")
	}

	next, _ = peekTags(reader, next, fields)
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeTagsPartial(t *testing.T) {
	var full bytes.Buffer
	writer := bufio.NewWriter(&full)
	tags := map[int]any{
		1: "test",
		2: CompactString("test2"),
		3: 123,
	}
	fields := map[int]*kafkaField{
		1: {valType: reflect.TypeOf(tags[1])},
		2: {valType: reflect.TypeOf(tags[2])},
	}
	encodeTags(writer, tags)
	writer.Flush()

	for i := 1 ; i < full.Len() - 1 ; i++ {
		var buf bytes.Buffer
		writer = bufio.NewWriter(&buf)
		writer.Write(full.Bytes()[:i])
		writer.Flush()

		reader := bufio.NewReader(&buf)
		next, _ := peekTags(reader, 0, fields)
		if next >= 0 {
			t.Error("didn't expect a value")
		}
	}
}

func TestEncodeDecodeEmpty(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	tags := map[int]any{}
	fields := map[int]*kafkaField{}
	encodeTags(writer, tags)
	encodeTags(writer, tags)
	writer.Flush()

	reader := bufio.NewReader(&buf)
	next, tg := peekTags(reader, 0, fields)
	if next < 0 {
		t.Error("expected a value")
	}
	if !reflect.DeepEqual(tags, tg) {
		t.Error("expected tag match")
	}

	next, tg = peekTags(reader, next, fields)
	if next < 0 {
		t.Error("expected a value")
	}
	if !reflect.DeepEqual(tags, tg) {
		t.Error("expected tag match")
	}

	next, _ = peekTags(reader, next, fields)
	if next >= 0 {
		t.Error("didn't expect a value")
	}
}

func TestEncodeDecodeTagsForeign(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	tags := map[int]any{
		1: "test",
		2: CompactString("test2"),
	}
	encodeTags(writer, tags)
	writer.Flush()

	reader := bufio.NewReader(&buf)
	next, tg := peekTags(reader, 0, map[int]*kafkaField{})
	if next < 0 {
		t.Error("expected a value")
	}
	expected := map[int]any{
		1: []byte{0,4,116,101,115,116},
		2: []byte{12,116,101,115,116,50},
	}
	if !reflect.DeepEqual(expected, tg) {
		t.Error("expected tag match")
	}
}

func TestEncodeDecodeApiVersions(t *testing.T) {
	ref := apiVersionsResponseV0{
		ApiKeys: []apiKeyV0{
			{ ApiKey: 1, MinVersion: 0, MaxVersion: 1 },
			{ ApiKey: 13, MinVersion: 1, MaxVersion: 6 },
		},
	}

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encodeObject(writer, ref)
	encodeObject(writer, &ref)
	writer.Flush()

	var zero apiVersionsResponseV0
	reader := bufio.NewReader(&buf)
	next, v := peekObject(reader, 0, reflect.TypeOf(zero))
	if next < 0 {
		t.Error("expected a value")
	}
	s, ok := v.(apiVersionsResponseV0)
	if !ok {
		t.Error("expected type")
	}
	if !reflect.DeepEqual(ref, s) {
		t.Error("expected value match")
	}

}