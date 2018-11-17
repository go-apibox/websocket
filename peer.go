package websocket

type Peer struct {
	handler Handler

	connHandlerFunc       func(conn *Conn)
	connClosedHandlerFunc func(conn *Conn)
	actionHandlerFuncMap  map[string]func(conn *Conn, packetDecoder *PacketDecoder)
}

type Handler interface {
	ServeConn(conn *Conn)
	ServeConnClosed(conn *Conn)
	ServeAction(action string, conn *Conn, packetDecoder *PacketDecoder)
}

func NewPeer() *Peer {
	peer := new(Peer)
	peer.actionHandlerFuncMap = make(map[string]func(conn *Conn, packetDecoder *PacketDecoder))
	return peer
}

func (peer *Peer) Handle(handler Handler) {
	peer.handler = handler

	// 使用handler时，禁用handlerFunc
	peer.connHandlerFunc = nil
	peer.connClosedHandlerFunc = nil
	peer.actionHandlerFuncMap = make(map[string]func(conn *Conn, packetDecoder *PacketDecoder))
}

// 连接成功处理
func (peer *Peer) HandleConnFunc(connHandler func(conn *Conn)) {
	peer.connHandlerFunc = connHandler

	// 使用handlerFunc时，禁用handler
	peer.handler = nil
}

// 连接关闭处理
func (peer *Peer) HandleConnClosedFunc(connClosedHandler func(conn *Conn)) {
	peer.connClosedHandlerFunc = connClosedHandler

	// 使用handlerFunc时，禁用handler
	peer.handler = nil
}

// 收到action包处理
func (peer *Peer) HandleActionFunc(action string, actionHandler func(conn *Conn, packetDecoder *PacketDecoder)) {
	peer.actionHandlerFuncMap[action] = actionHandler

	// 使用handlerFunc时，禁用handler
	peer.handler = nil
}
