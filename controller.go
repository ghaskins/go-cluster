package main

import (
	"fmt"
	"github.com/looplab/fsm"
)

type Controller struct {
	state             fsm.FSM
	connectionEvents  chan *Connection
	peers             IdentityMap
	id                *Identity
	activePeers       map[string]Peer
}

func NewController(_id *Identity, _peers IdentityMap) *Controller {
	return &Controller{
		connectionEvents: make(chan *Connection),
		peers: _peers,
		id: _id,
		activePeers: make(map[string]Peer),
	}
}

func (self *Controller) Connect(conn *Connection) {
	self.connectionEvents <- conn
}

func (self *Controller) Run() {

	disconnectionEvents := make(DisconnectChannel)
	messageEvents       := make(MessageChannel, 100)

	// Main engine
	for {
		select {
		case conn := <-self.connectionEvents:
			fmt.Printf("new connection from %s\n", conn.Id.Id)

			if _, ok := self.activePeers[conn.Id.Id]; ok {
				fmt.Printf("client is already connected")
				continue
			}

			peer := &Peer{conn: conn, rxChannel: &messageEvents, disconnectChannel: &disconnectionEvents}
			self.activePeers[conn.Id.Id] = *peer
			peer.Run()

			mode := Heartbeat_PING
			msg := &Heartbeat{Mode: &mode}
			peer.Send(msg)
		case msg := <-messageEvents:
			fmt.Printf("received msg: %s\n", msg.From.Id())
		case peerId := <-disconnectionEvents:
			fmt.Printf("lost connection from %s\n", peerId)
			delete(self.activePeers, peerId)
		}
	}
}