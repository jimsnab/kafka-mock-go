package kafkamock

import (
	"net"
	"sync"

	"github.com/jimsnab/go-lane"
)

type (
	kafkaMessage struct {
		l             lane.Lane
		conn          net.Conn
		port          uint
		correlationId int
		pmu           *sync.Mutex
	}
)

func newKafkaMessage(l lane.Lane, conn net.Conn, correlationId int, pmu *sync.Mutex) *kafkaMessage {
	return &kafkaMessage{
		l:             l,
		conn:          conn,
		port:          netRemotePort(conn),
		correlationId: correlationId,
		pmu:           pmu,
	}
}

func (km *kafkaMessage) send(payload []byte) (err error) {
	km.pmu.Lock()
	defer km.pmu.Unlock()

	payloadSize := 4 + len(payload)

	number := []byte{0, 0, 0, 0}
	convertInt32(number, int32(payloadSize))
	if _, err = km.conn.Write(number); err != nil {
		km.l.Errorf("error sending kafka %d response size: %v", km.port, err)
		return
	}

	convertInt32(number, int32(km.correlationId))
	if _, err = km.conn.Write(number); err != nil {
		km.l.Errorf("error sending kafka %d correlation id: %v", km.port, err)
		return
	}

	if _, err = km.conn.Write(payload); err != nil {
		km.l.Errorf("error sending kafka %d response: %v", km.port, err)
		return
	}

	km.l.Tracef("sent kafka %d response %d: %d bytes", km.port, km.correlationId, payloadSize+8)
	return
}
