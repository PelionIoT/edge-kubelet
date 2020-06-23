package net

import (
	"fmt"
	"net"

	"github.com/armPelionEdge/fog-proxy/protocol"
)

func DialNodeTCP(proxyAddress, node, address string) (net.Conn, error) {
	return (&FogProxyDialer{ProxyAddress: proxyAddress}).Dial("tcp", node, address)
}

type FogProxyDialer struct {
	ProxyAddress string
}

func (dialer *FogProxyDialer) Dial(network, node, address string) (net.Conn, error) {
	if network != "tcp" {
		return nil, fmt.Errorf("Unsupported network type: %s. Must be tcp", network)
	}

	conn, err := net.Dial("tcp", dialer.ProxyAddress)

	if err != nil {
		return nil, err
	}

	// Write MessageOpen frame
	fogProxyConn := &FogProxyConn{
		Conn:        conn,
		node:        node,
		nodeAddress: address,
	}

	var message protocol.Message
	message.Type = protocol.Message_Open
	message.Payload = &protocol.Message_OpenPayload{
		OpenPayload: &protocol.OpenPayload{
			Node:    node,
			Address: address,
		},
	}

	if err := fogProxyConn.writeMessage(message); err != nil {
		return nil, err
	}

	return fogProxyConn, nil
}
