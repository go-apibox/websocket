package websocket

import (
	"encoding/json"
)

type Packet struct {
	Header map[string]interface{} `json:"h,omitempty"`
	Action string                 `json:"a"`
	Data   interface{}            `json:"d,omitempty"`
	Error  *PacketError           `json:"e,omitempty"`
}

type PacketError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewPacket(action string) *Packet {
	packet := new(Packet)
	packet.Action = action
	return packet
}

func (packet *Packet) SetHeader(header map[string]interface{}) *Packet {
	packet.Header = header
	return packet
}

func (packet *Packet) SetData(data interface{}) *Packet {
	packet.Data = data
	return packet
}

func (packet *Packet) SetError(errCode, errMsg string) *Packet {
	err := new(PacketError)
	err.Code = errCode
	err.Message = errMsg
	packet.Error = err
	return packet
}

func (packet *Packet) Marshal() ([]byte, error) {
	return json.Marshal(packet)
}
