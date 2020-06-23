package net

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/armPelionEdge/fog-proxy/protocol"
	"github.com/golang/protobuf/proto"
)

// Unpack received data from message
// Pack sent data into message
type FogProxyConn struct {
	net.Conn
	node           string
	nodeAddress    string
	currentMessage *protocol.Message
	currentData    []byte
	closed         *protocol.ClosePayload
}

func (conn *FogProxyConn) Node() string {
	return conn.node
}

func (conn *FogProxyConn) NodeAddress() string {
	return conn.nodeAddress
}

func (conn *FogProxyConn) Read(b []byte) (n int, err error) {
	if err := conn.readNextMessage(); err != nil {
		return 0, err
	}

	switch conn.currentMessage.Type {
	case protocol.Message_Close:
		conn.Conn.Close()

		conn.currentMessage = nil
		conn.closed = conn.currentMessage.GetClosePayload()

		return 0, io.EOF
	case protocol.Message_Data:
		return conn.readMessageData(b), nil
	default:
		conn.currentMessage = nil

		return 0, fmt.Errorf("Unexpected message type encountered: %d", conn.currentMessage.Type)
	}
}

func (conn *FogProxyConn) readMessageData(b []byte) int {
	n := copy(b, conn.currentData)

	// Truncate read bytes from payload. If no more bytes are in this message
	// Then clear currentMessage so the next call to readNextMessage() will
	// parse the next message from the stream
	conn.currentData = conn.currentData[n:]

	if len(conn.currentData) == 0 {
		conn.currentMessage = nil
		conn.currentData = nil
	}

	return n
}

func (conn *FogProxyConn) readNextMessage() error {
	// If the last message has remaining data we don't want to read the next message
	// before consuming all data from currentMessage's payload
	if conn.currentMessage != nil {
		return nil
	}

	length, err := conn.readLength()

	if err != nil {
		return err
	}

	var buf []byte = make([]byte, length)

	if err := conn.fillBuffer(buf); err != nil {
		return err
	}

	var message protocol.Message

	if err := proto.Unmarshal(buf, &message); err != nil {
		return err
	}

	// Make sure the type matches the supplied payload
	switch message.Type {
	case protocol.Message_Close:
		if message.GetClosePayload() == nil {
			return fmt.Errorf("Close message received with no close payload")
		}
	case protocol.Message_Open:
		if message.GetOpenPayload() == nil {
			return fmt.Errorf("Open message received with no open payload")
		}
	case protocol.Message_Data:
		if message.GetDataPayload() == nil {
			return fmt.Errorf("Data message received with no data payload")
		}

		conn.currentData = message.GetDataPayload()
	}

	conn.currentMessage = &message

	return nil
}

func (conn *FogProxyConn) readLength() (uint64, error) {
	var length uint64

	err := binary.Read(conn.Conn, binary.LittleEndian, &length)

	return length, err
}

func (conn *FogProxyConn) fillBuffer(buf []byte) error {
	var nRead int = 0

	for nRead < len(buf) {
		n, err := conn.Conn.Read(buf[nRead:])

		if err != nil {
			return err
		}

		nRead += n
	}

	return nil
}

func (conn *FogProxyConn) Write(b []byte) (n int, err error) {
	// Encode message into envelope. Write raw TCP stream
	var message protocol.Message

	message.Type = protocol.Message_Data
	message.Payload = &protocol.Message_DataPayload{DataPayload: b}

	if err := conn.writeMessage(message); err != nil {
		return 0, err
	}

	return len(b), nil
}

func (conn *FogProxyConn) writeMessage(message protocol.Message) error {
	buf, err := proto.Marshal(&message)

	if err != nil {
		return err
	}

	// Write length
	var length uint64 = uint64(len(buf))

	if err := binary.Write(conn.Conn, binary.LittleEndian, &length); err != nil {
		return err
	}

	// Write payload
	if _, err := conn.Conn.Write(buf); err != nil {
		return err
	}

	return nil
}

func (conn *FogProxyConn) Shutdown(code protocol.ClosePayload_CloseCode, reason string) error {
	var message protocol.Message

	message.Type = protocol.Message_Close
	message.Payload = &protocol.Message_ClosePayload{ClosePayload: &protocol.ClosePayload{Code: code, Reason: reason}}

	conn.closed = message.GetClosePayload()

	if err := conn.writeMessage(message); err != nil {
		return err
	}

	return nil
}

func (conn *FogProxyConn) CloseReason() *protocol.ClosePayload {
	return conn.closed
}
