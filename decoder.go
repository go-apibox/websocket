package websocket

import (
	"encoding/json"
	"errors"
	"strings"
)

type PacketDecoder struct {
	*json.Decoder
	packetParsed   bool
	packetParseErr error
	action         string
	dataParsed     bool
	dataParseErr   error
	data           interface{}
}

func NewPacketDecoder(data []byte) *PacketDecoder {
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.UseNumber()
	decoder := new(PacketDecoder)
	decoder.Decoder = d
	return decoder
}

func (decoder *PacketDecoder) ParseAction() (string, error) {
	if decoder.packetParsed {
		return decoder.action, decoder.packetParseErr
	}
	decoder.packetParsed = true

	// 取 {
	t, err := decoder.Token()
	if err != nil {
		decoder.packetParseErr = err
		return "", err
	}
	if v, ok := t.(json.Delim); ok && v.String() == "{" {
		// 取Message
		t, err := decoder.Token()
		if err != nil {
			decoder.packetParseErr = err
			return "", err
		}
		if v, ok := t.(string); ok && v == "a" {
			t, err := decoder.Token()
			if err != nil {
				decoder.packetParseErr = err
				return "", err
			}
			if action, ok := t.(string); ok {
				decoder.action = action
				return action, nil
			} else {
				decoder.packetParseErr = errors.New("invalid action value")
				return "", decoder.packetParseErr
			}
		}
	}

	decoder.packetParseErr = errors.New("expect action key")
	return "", decoder.packetParseErr
}

func (decoder *PacketDecoder) ParseData(data interface{}) error {
	if !decoder.packetParsed {
		return errors.New("action not parsed")
	}
	if decoder.dataParsed {
		data = decoder.data
		return decoder.dataParseErr
	}
	decoder.dataParsed = true

	// 取Data
	t, err := decoder.Token()
	if err != nil {
		decoder.dataParseErr = err
		return err
	}
	if v, ok := t.(string); ok && v == "d" {
		if err := decoder.Decode(data); err != nil {
			decoder.dataParseErr = err
			return err
		} else {
			decoder.data = data
			return nil
		}
	}

	decoder.dataParseErr = errors.New("expect data key")
	return decoder.dataParseErr
}
