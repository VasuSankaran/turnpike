package turnpike

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type websocketPeer struct {
	conn        *websocket.Conn
	serializer  Serializer
	messagesIn  chan Message
	messagesOut chan Message
	outChBlocks bool
	payloadType int
	closed      bool
	closedLock  sync.Mutex
	done        chan struct{}
	sendMutex   sync.Mutex
}

func NewWebsocketPeer(serialization Serialization, url string, requestHeader http.Header, tlscfg *tls.Config, dial DialFunc) (Peer, error) {
	switch serialization {
	case JSON:
		return newWebsocketPeer(url, requestHeader, jsonWebsocketProtocol,
			new(JSONSerializer), websocket.TextMessage, tlscfg, dial,
		)
	case MSGPACK:
		return newWebsocketPeer(url, requestHeader, msgpackWebsocketProtocol,
			new(MessagePackSerializer), websocket.BinaryMessage, tlscfg, dial,
		)
	default:
		return nil, fmt.Errorf("Unsupported serialization: %v", serialization)
	}
}

func newWebsocketPeer(url string, reqHeader http.Header, protocol string, serializer Serializer, payloadType int, tlscfg *tls.Config, dial DialFunc) (Peer, error) {
	dialer := websocket.Dialer{
		Subprotocols:    []string{protocol},
		TLSClientConfig: tlscfg,
		Proxy:           http.ProxyFromEnvironment,
		NetDial:         dial,
	}
	conn, _, err := dialer.Dial(url, reqHeader)
	if err != nil {
		return nil, err
	}
	ep := &websocketPeer{
		conn:        conn,
		messagesIn:  make(chan Message, 10),
		messagesOut: make(chan Message, 10),
		done:        make(chan struct{}),
		outChBlocks: true,
		serializer:  serializer,
		payloadType: payloadType,
	}
	go ep.runReadMessages()
	go ep.runWriteMessages()

	return ep, nil
}

func (ep *websocketPeer) Send(msg Message) error {
	if ep.outChBlocks {
		select {
		case <-ep.done:
		case ep.messagesOut <- msg:
		}
	} else {
		// To keep slow clients from impacting other clients, the server will drop messages if the buffer is full.
		select {
		case <-ep.done:
		case ep.messagesOut <- msg:
		default:
			log.Println("websocket messagesOut is full so Server dropping msg")
		}
	}

	return nil
}

func (ep *websocketPeer) doWriteMessage(msg Message) error {
	b, err := ep.serializer.Serialize(msg)
	if err != nil {
		return err
	}
	ep.sendMutex.Lock()
	defer ep.sendMutex.Unlock()
	return ep.conn.WriteMessage(ep.payloadType, b)
}

func (ep *websocketPeer) Receive() <-chan Message {
	return ep.messagesIn
}
func (ep *websocketPeer) Close() error {
	closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "goodbye")
	err := ep.conn.WriteControl(websocket.CloseMessage, closeMsg, time.Now().Add(5*time.Second))
	if err != nil {
		log.Println("error sending close message:", err)
	}
	ep.closedLock.Lock()
	defer ep.closedLock.Unlock()
	if !ep.closed {
		close(ep.done)
		ep.closed = true
	}
	return ep.conn.Close()
}

func (ep *websocketPeer) runReadMessages() {
	for {
		// TODO: use conn.NextMessage() and stream
		// TODO: do something different based on binary/text frames
		if msgType, b, err := ep.conn.ReadMessage(); err != nil {
			select {
			case <-ep.done:
				log.Println("peer connection closed")
			default:
				log.Println("error reading from peer:", err)
				ep.conn.Close()
			}
			close(ep.messagesIn)
			break
		} else if msgType == websocket.CloseMessage {
			ep.conn.Close()
			close(ep.messagesIn)
			break
		} else {
			msg, err := ep.serializer.Deserialize(b)
			if err != nil {
				log.Println("error deserializing peer message:", err)
				// TODO: handle error
			} else {
				ep.messagesIn <- msg
			}
		}
	}
}

func (ep *websocketPeer) runWriteMessages() {
	for {
		select {
		case msg := <-ep.messagesOut:
			ep.doWriteMessage(msg)
		case <-ep.done:
			return
		}
	}
}
