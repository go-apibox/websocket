package websocket

import (
	"github.com/go-apibox/api"
)

type Server struct {
	*Peer
}

func NewServer() *Server {
	server := new(Server)
	server.Peer = NewPeer()
	return server
}

func (server *Server) Serve(c *api.Context) error {
	wsConn, err := c.UpgradeWebsocket()
	if err != nil {
		return err
	}
	conn := NewConn(wsConn, c.Request().Form)
	return conn.Serve(server.Peer)
}
