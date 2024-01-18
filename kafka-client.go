package kafkamock

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jimsnab/go-lane"
)

type (
	kafkaClient struct {
		l          lane.Lane
		conn       net.Conn
		serverPort uint
		clientPort uint
		oc         onClose
		inbound    []byte
		latency    time.Duration
		connected  sync.WaitGroup
		ds         *kafkaDataStore
		requests   []*kafkaRequest
		wg         sync.WaitGroup // overall running state
		requestWg  sync.WaitGroup // active api request
		stopping   atomic.Bool
	}

	onClose func()

	kafkaMessageHeader struct {
		RequestApiKey     kafkaApiKey
		RequestApiVersion int
		CorrelationId     int
		Client            string
		Tags              map[int]any
	}

	kafkaHeader0 struct {
		RequestApiKey     int16
		RequestApiVersion int16
		CorrelationId     int32
	}

	dispatchHandler func(reader *bufio.Reader, kc *kafkaClient, kmh *kafkaMessageHeader) (response any, rtags map[int]any, err error)

	kafkaApiKey int

	kafkaRequest struct {
	}
)

func newKafkaClient(l lane.Lane, ds *kafkaDataStore, conn net.Conn, serverPort uint, latency time.Duration, oc onClose) *kafkaClient {
	kc := &kafkaClient{
		l:          l,
		conn:       conn,
		clientPort: netRemotePort(conn),
		serverPort: serverPort,
		oc:         oc,
		inbound:    []byte{},
		ds:         ds,
		latency:    latency,
		requests:   []*kafkaRequest{},
	}

	kc.wg.Add(1)
	go kc.handle()
	return kc
}

func (kc *kafkaClient) handle() {
	defer func() {
		kc.wg.Done()
		kc.oc()
		kc.l.Tracef("client %d task done", kc.clientPort)
	}()

	kc.connected.Add(1)
	defer kc.connected.Done()

	for {
		if kc.latency != 0 {
			time.Sleep(kc.latency) // test hook
		}

		msg := getMessage(kc.inbound)
		if msg == nil {
			select {
			case <-kc.l.Done():
				// don't accept more requests
				kc.conn.Close()
				return
			default:
				// keep going
			}

			// no message ready - read more data from the client
			buffer := make([]byte, 8192)
			kc.conn.SetReadDeadline(time.Now().Add(time.Second))
			n, err := kc.conn.Read(buffer)

			if err != nil {
				if errors.Is(err, os.ErrDeadlineExceeded) {
					continue
				}
				if !wasSocketClosed(err) {
					panic(fmt.Sprintf("read error: %v", err))
				}
				return
			}

			if kc.stopping.Load() {
				// ignore the continued requests
				kc.l.Tracef("closing in progress - ignoring %d request bytes", n)
				continue
			}

			kc.inbound = append(kc.inbound, buffer[:n]...)
			continue
		}

		kc.inbound = kc.inbound[4+msg.Len():]

		kc.requestWg.Add(1)
		reader := bufio.NewReader(msg)
		func() {
			defer kc.requestWg.Done()
			if err := kc.dispatcher(reader, msg.Len()); err != nil {
				panic(err)
			}
		}()
	}
}

func (kc *kafkaClient) Close() {
	// prevent starting work on more requests
	kc.stopping.Store(true)

	// wait for the in flight requests to complete
	kc.l.Tracef("waiting for client %d in-flight requests to complete", kc.clientPort)
	kc.requestWg.Wait()

	// close communication with the client
	kc.conn.Close()

	// wait for the handler task to finish
	kc.l.Tracef("waiting for client %d handler task", kc.clientPort)
	kc.wg.Wait()

	kc.l.Tracef("client %d closed", kc.clientPort)
}

func (kc *kafkaClient) String() string {
	return kc.conn.RemoteAddr().String()
}

func (kc *kafkaClient) dispatcher(reader *bufio.Reader, msgLength int) (err error) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	var hdr kafkaHeader0
	next, obj := peekObject(reader, 0, reflect.TypeOf(hdr))
	if next < 0 {
		err = fmt.Errorf("header invalid")
		return
	}
	hdr, _ = obj.(kafkaHeader0)

	apiName := apiNames[kafkaApiKey(hdr.RequestApiKey)]

	kc.l.Tracef("kafka %d request %d: %s v%d", kc.clientPort, hdr.CorrelationId, apiName, hdr.RequestApiVersion)

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
		kc.l.Tracef("kafka %d request tags: %v", kc.clientPort, tags)
	}

	kmh := kafkaMessageHeader{
		RequestApiKey:     kafkaApiKey(hdr.RequestApiKey),
		RequestApiVersion: int(hdr.RequestApiVersion),
		CorrelationId:     int(hdr.CorrelationId),
		Client:            clientId,
		Tags:              tags,
	}

	handler, defined := apiTable[k]
	if !defined {
		err = fmt.Errorf("api undefined")
		kc.l.Warnf("kafka request %d for unsupported API %d %s v%d", kc.clientPort, hdr.RequestApiKey, apiName, hdr.RequestApiVersion)
		return
	}

	reader.Discard(next)

	var response any
	var rtags map[int]any
	response, rtags, err = handler(reader, kc, &kmh)
	if err != nil {
		return
	}

	remaining, _ := reader.Peek(msgLength)
	if len(remaining) != 0 {
		err = fmt.Errorf("unexpected %d bytes after %T request", len(remaining), response)
		return
	}

	encodeObject(w, response)
	if rtags != nil {
		encodeTags(w, rtags)
	}

	// message fully processed
	w.Flush()

	km := newKafkaMessage(kc.l, kc.conn, &kmh)
	if err = km.send(buf.Bytes()); err != nil {
		if !kc.isConnected() {
			// the client dropped - ignore the error
			err = nil
		}
		return
	}

	return
}

func (kc *kafkaClient) isConnected() bool {
	f, err := kc.conn.(*net.TCPConn).File()
	if err != nil {
		return false
	}

	b := []byte{0}
	_, _, err = syscall.Recvfrom(int(f.Fd()), b, syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
	return err != nil
}

func netRemotePort(conn net.Conn) uint {
	addr := conn.RemoteAddr()
	tcpAddr, is := addr.(*net.TCPAddr)
	if !is {
		panic("unsupported net connection type")
	}

	return uint(tcpAddr.Port)
}
