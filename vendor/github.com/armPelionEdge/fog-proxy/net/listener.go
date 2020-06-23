package net

import (
	"net"

	"github.com/armPelionEdge/fog-proxy/protocol"
	"github.com/golang/protobuf/proto"
)

func Listen(address string) (*FogProxyListener, error) {
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return nil, err
	}

	return &FogProxyListener{Listener: listener}, nil
}

type FogProxyListener struct {
	net.Listener
}

func (listener *FogProxyListener) Accept() (net.Conn, error) {
	conn, err := listener.Listener.Accept()

	if err != nil {
		return conn, err
	}

	fogProxyConn := &FogProxyConn{
		Conn: conn,
	}

	length, err := fogProxyConn.readLength()

	if err != nil {
		conn.Close()

		return nil, err
	}

	var buf []byte = make([]byte, length)

	if err := fogProxyConn.fillBuffer(buf); err != nil {
		conn.Close()

		return nil, err
	}

	var message protocol.Message

	if err := proto.Unmarshal(buf, &message); err != nil {
		fogProxyConn.Shutdown(protocol.ClosePayload_ProtocolError, "First message was not an open message")
		fogProxyConn.Close()

		return nil, err
	}

	switch message.Type {
	case protocol.Message_Open:
		if message.GetOpenPayload() == nil {
			fogProxyConn.Shutdown(protocol.ClosePayload_ProtocolError, "First message was an open message with an invalid payload")
			fogProxyConn.Close()

			return nil, err
		}

		fogProxyConn.node = message.GetOpenPayload().Node
		fogProxyConn.nodeAddress = message.GetOpenPayload().Address
	default:
		fogProxyConn.Shutdown(protocol.ClosePayload_ProtocolError, "First message was not an open message")
		fogProxyConn.Close()

		return nil, err
	}

	return fogProxyConn, nil
}
