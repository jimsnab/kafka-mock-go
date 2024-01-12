package kafkamock

import (
	"net"

	"github.com/jimsnab/go-lane"
)

type (
	kafkaMessage struct {
		l             lane.Lane
		conn          net.Conn
		correlationId int
	}
)

func newKafkaMessage(l lane.Lane, conn net.Conn, correlationId int) *kafkaMessage {
	return &kafkaMessage{
		l:             l,
		conn:          conn,
		correlationId: correlationId,
	}
}

func (km *kafkaMessage) send(payload []byte) (err error) {
	payloadSize := 4 + len(payload)

	number := []byte{0, 0, 0, 0}
	convertInt32(number, int32(payloadSize))
	if _, err = km.conn.Write(number); err != nil {
		km.l.Errorf("error sending kafka response size: %v", err)
		return
	}

	convertInt32(number, int32(km.correlationId))
	if _, err = km.conn.Write(number); err != nil {
		km.l.Errorf("error sending kafka correlation id: %v", err)
		return
	}

	if _, err = km.conn.Write(payload); err != nil {
		km.l.Errorf("error sending kafka response: %v", err)
		return
	}

	km.l.Tracef("sent kafka response %d: %d bytes", km.correlationId, payloadSize+8)
	return
}
