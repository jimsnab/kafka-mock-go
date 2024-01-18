package kafkamock

import (
	"fmt"
	"net"

	"github.com/jimsnab/go-lane"
)

type (
	kafkaMessage struct {
		l    lane.Lane
		conn net.Conn
		port uint
		hdr  *kafkaMessageHeader
	}
)

func newKafkaMessage(l lane.Lane, conn net.Conn, hdr *kafkaMessageHeader) *kafkaMessage {
	return &kafkaMessage{
		l:    l,
		conn: conn,
		port: netRemotePort(conn),
		hdr:  hdr,
	}
}

func (km *kafkaMessage) send(payload []byte) (err error) {
	payloadSize := 4 + len(payload)

	number := []byte{0, 0, 0, 0}
	convertInt32(number, int32(payloadSize))
	if _, err = km.conn.Write(number); err != nil {
		km.l.Errorf("error sending kafka %d response size: %v", km.port, err)
		return
	}

	convertInt32(number, int32(km.hdr.CorrelationId))
	if _, err = km.conn.Write(number); err != nil {
		km.l.Errorf("error sending kafka %d correlation id: %v", km.port, err)
		return
	}

	if _, err = km.conn.Write(payload); err != nil {
		km.l.Errorf("error sending kafka %d response: %v", km.port, err)
		return
	}

	cmd, _ := apiNames[km.hdr.RequestApiKey]
	if cmd == "" {
		cmd = fmt.Sprintf("%d", km.hdr.RequestApiKey)
	}
	km.l.Tracef("sent kafka %d response %d: cmd %s v%d: %d bytes", km.port, km.hdr.CorrelationId, cmd, km.hdr.RequestApiVersion, payloadSize+8)
	return
}
