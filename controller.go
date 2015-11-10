package main

import (
	"crypto/rand"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/looplab/fsm"
	"time"
)

type Controller struct {
	state            *fsm.FSM
	connectionEvents chan *Connection
	peers            IdentityMap
	myId             string
	activePeers      map[string]Peer
	quorumThreshold  int
	viewId           int64
	timer            *time.Timer
	electionManager  *ElectionManager
}

func NewController(_id string, _peers IdentityMap) *Controller {

	var members []string

	for _, peer := range _peers {
		members = append(members, peer.Id)
	}

	self := &Controller{
		connectionEvents: make(chan *Connection),
		peers:            _peers,
		myId:             _id,
		activePeers:      make(map[string]Peer),
		quorumThreshold:  ComputeQuorumThreshold(len(_peers)) - 1, // We don't include ourselves
		electionManager:  NewElectionManager(_id, members),
	}

	self.state = fsm.NewFSM(
		"initializing",
		fsm.Events{
			{Name: "elected-self", Src: []string{"initializing", "electing"}, Dst: "leading"},
			{Name: "elected-other", Src: []string{"initializing", "electing"}, Dst: "following"},
			{Name: "timeout", Src: []string{"initializing", "following"}, Dst: "electing"},
			{Name: "election", Src: []string{"following", "leading"}, Dst: "electing"},
			{Name: "heartbeat", Src: []string{"following"}, Dst: "following"},
		},
		fsm.Callbacks{
			"enter_initializing": func(e *fsm.Event) { self.rearmTimer() },
			"leave_initializing": func(e *fsm.Event) { self.timer.Stop() },
			"enter_following":    func(e *fsm.Event) { self.rearmTimer() },
			"leave_following":    func(e *fsm.Event) { self.timer.Stop() },
			"heartbeat":          func(e *fsm.Event) { self.onHeartBeat(e.Args[0].(string), e.Args[1].(int64)) },
			"electing":           func(e *fsm.Event) { self.onElecting() },
			"timeout":            func(e *fsm.Event) { self.onTimeout() },
		},
	)

	return self
}

func (self *Controller) Connect(conn *Connection) {
	self.connectionEvents <- conn
}

func (self *Controller) Run() {

	disconnectionEvents := make(DisconnectChannel)
	messageEvents := make(MessageChannel, 100)

	// Main engine
	for {
		select {

		//---------------------------------------------------------
		// new connections
		//---------------------------------------------------------
		case conn := <-self.connectionEvents:
			fmt.Printf("new connection from %s\n", conn.Id.Id)

			if _, ok := self.activePeers[conn.Id.Id]; ok {
				fmt.Printf("client is already connected")
				continue
			}

			peer := &Peer{conn: conn, rxChannel: &messageEvents, disconnectChannel: &disconnectionEvents}
			self.activePeers[conn.Id.Id] = *peer
			peer.Run()

			if len(self.activePeers) == self.quorumThreshold {
				self.state.Event("quorum-acquired")
			}

			// Update the peer with an unsolicited vote if we already have an opinion on who is leader
			leader, err := self.electionManager.Current()
			if err != nil {
				msg := &Vote{
					ViewId: &self.viewId,
					PeerId: &leader,
				}
				peer.Send(msg)
			}

		//---------------------------------------------------------
		// message arrival
		//---------------------------------------------------------
		case _msg := <-messageEvents:
			switch _msg.Payload.(type) {
			case *Heartbeat:
				msg := *Heartbeat(_msg.Payload)
				self.state.Event("heartbeat", _msg.From.Id(), msg.GetViewId())
			case *Vote:
				msg := *Vote(_msg.Payload)
				self.electionManager.ProcessVote(_msg.From.Id(), msg)
			}

		//---------------------------------------------------------
		// leader election
		//---------------------------------------------------------
		case val := <-self.electionManager.C:

			if val {
				// val == true means we elected a new leader
				leader, err := self.electionManager.Current()
				if err != nil {
					if leader == self.myId {
						self.state.Event("elected-self")
					} else {
						self.state.Event("elected-other")
					}
				}
			} else {
				// val == false means we started a new election
				self.state.Event("election")
			}

		//---------------------------------------------------------
		// timeouts
		//---------------------------------------------------------
		case _ := <-self.timer:
			self.state.Event("timeout")

		//---------------------------------------------------------
		// disconnects
		//---------------------------------------------------------
		case peerId := <-disconnectionEvents:
			fmt.Printf("lost connection from %s\n", peerId)
			if len(self.activePeers) == self.quorumThreshold {
				self.state.Event("quorum-lost")
			}
			delete(self.activePeers, peerId)
			self.electionManager.Invalidate(peerId)
		}
	}
}

func (self *Controller) rearmTimer() {
	offset, err := rand.Int(rand.Reader, 150)
	if err != nil {
		panic("bad return from rand.Int")
	}

	self.timer = time.NewTimer(time.Millisecond * (150 + offset)).C
}

func (self *Controller) onHeartBeat(from string, viewId int64) {
	leader, err := self.electionManager.Current()
	if err == nil {
		panic(err)
	}

	// Only pet the watchdog if the HB originated from the node we believe to be the leader
	if from == leader && viewId == self.viewId {
		self.rearmTimer()
	}
}

func (self *Controller) onTimeout() {

	if self.electionManager.VoteCount() == 0 {
		vote := &Vote{ViewId: self.viewId + 1, PeerId: self.myId}

		self.electionManager.ProcessVote(self.myId, vote)
	}
}

func (self *Controller) onElecting() {

	vote, err := self.electionManager.GetContender()
	if err != nil {
		panic(err)
	}

	self.broadcast(vote)
}

func (self *Controller) broadcast(msg proto.Message) {
	for _, peer := range self.activePeers {
		peer.Send(msg)
	}
}
