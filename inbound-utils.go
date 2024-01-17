package kafkamock

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

type (
	kafkaField struct {
		name            string
		docString       string
		valType         reflect.Type
		hasDefaultValue bool
		defaultValue    any
	}
)

func getMessage(inbound []byte) (buf *bytes.Buffer) {
	if len(inbound) < 4 {
		return
	}

	msgSize := binary.BigEndian.Uint32(inbound)
	if len(inbound) < (int(msgSize) + 4) {
		return
	}

	buf = bytes.NewBuffer(inbound[4 : 4+msgSize])
	return
}

func readRequest[T any](reader *bufio.Reader) (obj *T, err error) {
	var request T
	next, r := peekObject(reader, 0, reflect.TypeOf(request))
	if next < 0 {
		err = fmt.Errorf("bad request %T", request)
		return
	}
	request, ok := r.(T)
	if !ok {
		panic("unexpected request object type")
	}
	reader.Discard(next)

	obj = &request
	return
}
