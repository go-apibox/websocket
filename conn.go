package websocket

import (
	"net/url"
	"sync"
	"time"

	ws "github.com/gorilla/websocket"
)

type Conn struct {
	wsConn *ws.Conn
	params url.Values
	mutex  *sync.Mutex
}

func NewConn(wsConn *ws.Conn, params url.Values) *Conn {
	c := new(Conn)
	c.wsConn = wsConn
	c.params = params
	c.mutex = new(sync.Mutex)
	return c
}

func (conn *Conn) Params() url.Values {
	return conn.params
}

func (conn *Conn) Write(data string) error {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	return conn.wsConn.WriteMessage(ws.TextMessage, []byte(data))
}

func (conn *Conn) Send(packet *Packet) error {
	data, err := packet.Marshal()
	if err != nil {
		return err
	}

	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	return conn.wsConn.WriteMessage(ws.TextMessage, data)
}

func (conn *Conn) Close() error {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	conn.wsConn.WriteControl(ws.CloseMessage,
		ws.FormatCloseMessage(ws.CloseNormalClosure, "close wsconn"),
		time.Time{})
	if err := conn.wsConn.Close(); err != nil {
		return err
	}
	return nil
}

func (conn *Conn) Serve(peer *Peer) error {
	defer func() {
		conn.Close()

		if peer.connClosedHandlerFunc != nil {
			peer.connClosedHandlerFunc(conn)
		} else if peer.handler != nil {
			peer.handler.ServeConnClosed(conn)
		}
	}()

	if peer.connHandlerFunc != nil {
		peer.connHandlerFunc(conn)
	} else if peer.handler != nil {
		peer.handler.ServeConn(conn)
	}

	for {
		msgType, data, err := conn.wsConn.ReadMessage()
		if err != nil {
			return err
		}

		if msgType != ws.TextMessage {
			continue
		}

		packetDecoder := NewPacketDecoder(data)
		action, err := packetDecoder.ParseAction()
		if err != nil {
			continue
		}

		if handlerFunc, has := peer.actionHandlerFuncMap[action]; has {
			handlerFunc(conn, packetDecoder)
		} else if peer.handler != nil {
			peer.handler.ServeAction(action, conn, packetDecoder)
		}
	}
}
