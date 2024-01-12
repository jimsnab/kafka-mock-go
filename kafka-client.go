package kafkamock

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"reflect"

	"github.com/jimsnab/go-lane"
)

type (
	kafkaClient struct {
		l       lane.Lane
		conn    net.Conn
		port    int
		oc      onClose
		inbound []byte
		ds      *kafkaDataStore
	}

	onClose func()

	kafkaHeader0 struct {
		RequestApiKey     int16
		RequestApiVersion int16
		CorrelationId     int32
	}

	dispatchHandler func(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error)

	kafkaApiKey int
)

func newKafkaClient(l lane.Lane, ds *kafkaDataStore, conn net.Conn, port int, oc onClose) *kafkaClient {
	kc := &kafkaClient{
		l:       l,
		conn:    conn,
		port:    port,
		oc:      oc,
		inbound: []byte{},
		ds:      ds,
	}

	go kc.handle()
	return kc
}

func (kc *kafkaClient) handle() {
	defer kc.oc()

	for {
		buffer := make([]byte, 8192)
		n, err := kc.conn.Read(buffer)
		if err != nil {
			if !wasSocketClosed(err) {
				panic(fmt.Sprintf("read error: %v", err))
			}
			return
		}

		kc.inbound = append(kc.inbound, buffer[0:n]...)

		if len(kc.inbound) < 4 {
			continue
		}

		msg := getMessage(kc.inbound)
		if msg == nil {
			continue
		}

		kc.inbound = kc.inbound[4+msg.Len():]
		reader := bufio.NewReader(msg)

		if err = kc.dispatcher(reader); err != nil {
			panic(err)
		}
	}
}

func (kc *kafkaClient) Close() {
	kc.conn.Close()
}

func (kc *kafkaClient) String() string {
	return kc.conn.RemoteAddr().String()
}

func (kc *kafkaClient) dispatcher(reader *bufio.Reader) (err error) {

	var hdr kafkaHeader0
	next, obj := peekObject(reader, 0, reflect.TypeOf(hdr))
	if next < 0 {
		err = fmt.Errorf("header invalid")
		return
	}
	hdr, _ = obj.(kafkaHeader0)

	apiName := apiNames[kafkaApiKey(hdr.RequestApiKey)]

	kc.l.Tracef("kafka request %d: %s v%d", hdr.CorrelationId, apiName, hdr.RequestApiVersion)

	next, clientId := peekString(reader, next)
	if next < 0 {
		err = fmt.Errorf("header client id invalid")
		return
	}

	k := makeApiKey(kafkaApiKey(hdr.RequestApiKey), int(hdr.RequestApiVersion))

	var tags map[int]any
	hasTags := apiHasTags[k]
	if hasTags {
		next, tags = peekTags(reader, next, map[int]*kafkaField{})
		if next < 0 {
			err = fmt.Errorf("header invalid tags")
			return
		}
		kc.l.Tracef("kafka request tags: %v", tags)
	}

	handler, defined := apiTable[k]
	if !defined {
		err = fmt.Errorf("api undefined")
		kc.l.Warnf("kafka request for unsupported API %d %s v%d", hdr.RequestApiKey, apiName, hdr.RequestApiVersion)
		return
	}

	reader.Discard(next)

	response, rtags, err := handler(reader, kc, clientId, tags)
	if err != nil {
		return
	}

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	encodeObject(w, response)

	if rtags != nil {
		encodeTags(w, rtags)
	}

	w.Flush()

	km := newKafkaMessage(kc.l, kc.conn, int(hdr.CorrelationId))
	if err = km.send(buf.Bytes()); err != nil {
		return
	}

	return
}
