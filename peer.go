package main

import (
	"fmt"
	"errors"
	"io"
	"github.com/golang/protobuf/proto"
)

type MessageChannel chan Message
type DisconnectChannel chan string

type Peer struct {
	conn              *Connection
	rxChannel         *MessageChannel
	txChannel          chan proto.Message
	disconnectChannel *DisconnectChannel
}

type Message struct {
	From    *Peer
	Payload proto.Message
}

func (self *Peer) Id() string {
	return self.conn.Id.Id
}

func (self *Peer) rxLoop() error {

	for {
		header := &Header{}
		if err := self.conn.Recv(header); err != nil {
			switch {
			case err == io.EOF:
				return nil
			default:
				return errors.New(fmt.Sprintf("header recv error %s", err.Error()))
			}

		}

		var payload proto.Message

		switch header.GetType() {
		case Type_HEARTBEAT:
			payload = new(Heartbeat)
		default:
			continue
		}

		if err := self.conn.Recv(payload); err != nil {
			return errors.New(fmt.Sprintf("payload recv error %s", err.Error()))
		}

		*self.rxChannel <- Message{From: self, Payload: payload}
	}
}

func (self *Peer) runRx() {
	err := self.rxLoop()
	if err != nil {
		fmt.Printf("%s: %s", self.conn.Id.Id, err.Error())
	}

	*self.disconnectChannel <- self.conn.Id.Id
}

func (self *Peer) runTx() {
	for {
		msg := <- self.txChannel
		fmt.Printf("tx message %v", msg)
	}
}

func (self *Peer) Run() {
	self.txChannel = make(chan proto.Message, 100)
	go self.runRx()
	go self.runTx()
}