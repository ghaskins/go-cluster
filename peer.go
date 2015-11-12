package main

import (
	"errors"
	"fmt"
	"github.com/ghaskins/go-cluster/pb"
	"github.com/golang/protobuf/proto"
	"io"
)

type MessageChannel chan Message
type DisconnectChannel chan string

type Peer struct {
	conn              *Connection
	rxChannel         *MessageChannel
	txChannel         chan proto.Message
	txStop            chan bool
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
		header := &pb.Header{}
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
		case pb.Type_HEARTBEAT:
			payload = new(pb.Heartbeat)
		case pb.Type_VOTE:
			payload = new(pb.Vote)
		default:
			continue
		}

		if err := self.conn.Recv(payload); err != nil {
			switch {
			case err == io.EOF:
				return nil
			default:
				return errors.New(fmt.Sprintf("payload recv error %s", err.Error()))
			}
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
	self.txStop <- true
}

func (self *Peer) runTx() {
	for {
		select {
		case msg := <-self.txChannel:

			var t pb.Type

			switch msg.(type) {
			case *pb.Heartbeat:
				t = pb.Type_HEARTBEAT
			case *pb.Vote:
				t = pb.Type_VOTE
			}

			header := &pb.Header{Type: &t}
			self.conn.Send(header)
			self.conn.Send(msg)
		case _ = <-self.txStop:
			return
		}
	}
}

func (self *Peer) Run() {
	self.txChannel = make(chan proto.Message, 100)
	self.txStop = make(chan bool)
	go self.runRx()
	go self.runTx()
}

func (self *Peer) Send(msg proto.Message) {
	// We send it indirectly on a channel so that the header+payload transfer is atomic
	self.txChannel <- msg
}
