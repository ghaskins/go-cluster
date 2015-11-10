package main

import (
	"fmt"
	"math"
	"time"
	"crypto/rand"
	"github.com/looplab/fsm"
	"github.com/golang/protobuf/proto"
)

type View struct {
	id int
	leader string
}

type Votes map[string]*Vote

type Controller struct {
	state             fsm.FSM
	connectionEvents  chan *Connection
	peers             IdentityMap
	myId             *Identity
	activePeers       map[string]Peer
	quorumThreshold   int
	view              View
	timer            *time.Timer
	votes             Votes
	electionManager  *ElectionManager
}

func NewController(_id *Identity, _peers IdentityMap) *Controller {

	var members []string

	for _,peer := range _peers {
		append(members, peer.Id)
	}

	self := &Controller{
		connectionEvents: make(chan *Connection),
		peers: _peers,
		myId: _id,
		activePeers: make(map[string]Peer),
		quorumThreshold: ComputeQuorumThreshold(len(_peers)) - 1, // We don't include ourselves
		timer: make(chan time.Time),
		votes: make(Votes),
		electionManager: NewElectionManager(_id, members),
	}


	self.state = fsm.NewFSM(
		"sensing",
		fsm.Events{
			{Name: "quorum-acquired", Src: []string{"sensing"},  Dst: "following"},
			{Name: "quorum-lost",   						   Dst: "sensing"},
			{Name: "elected-self",       Src: []string{"sensing", "electing"}, Dst: "leading"},
			{Name: "elected-other",       Src: []string{"sensing", "electing"}, Dst: "following"},
			{Name: "timeout",       Src: []string{"following"}, Dst: "electing"},
			{Name: "election",       Src: []string{"sensing", "following", "leading"}, Dst: "electing"},

			{Name: "candidate",       Src: []string{"following"}, Dst: "candidate"},
			{Name: "ping",          Src: []string{"following", "candidate"}, Dst: "following"},
			{Name: "ping",          Src: []string{"leading"}, Dst: "candidate"},
			{Name: "elected",       Src: []string{"candidate"}, Dst: "leading"},
			{Name: "yield",         Src: []string{"candidate"}, Dst: "following"},
			{Name: "vote",          Src: []string{"leading"}, Dst: "candidate"},
		},
		fsm.Callbacks{
			"enter_following": func(e *fsm.Event) { self.rearmTimer() },
			"leave_following": func(e *fsm.Event) { self.timer.Stop() },

		},
	)

	return self

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

			if (len(self.activePeers) == self.quorumThreshold) {
				self.state.Event("quorum-acquired")
			}

			// Update the peer with an unsolicited vote if we already have an opinion on who is leader
			if self.view.leader != nil {
				msg := &Vote{
					ViewId: self.view.id,
					PeerId: self.view.leader,
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
				self.onHeartBeat(_msg.From.Id(), msg.GetViewId())
			case *Vote:
				msg := *Vote(_msg.Payload)
				self.electionManager.Vote(_msg.From.Id(), msg)
			}

		//---------------------------------------------------------
		// leader election
		//---------------------------------------------------------
		case val := <-self.electionManager.C:

			if val {
				// val == true means we elected a new leader
				self.view.leader = self.electionManager.Current()
				if self.view.leader == self.myId {
					self.state.Event("elected-self")
				} else {
					self.state.Event("elected-other")
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
			if (len(self.activePeers) == self.quorumThreshold) {
				self.state.Event("quorum-lost")
			}
			delete(self.activePeers, peerId)
			self.electionManager.Invalidate(peerId)
		}
	}
}

func (self *Controller) rearmTimer() {
	offset,err := rand.Int(rand.Reader, 150)
	if err != nil {
		panic("bad return from rand.Int")
	}

	self.timer = time.NewTimer(time.Millisecond * (150 + offset)).C
}

func (self *Controller) onHeartBeat(from string, viewId int64) {
	switch self.state.Current() {
	case "following":

		// Only pet the watchdog if the HB originated from the node we believe to be the leader
		if (from == self.view.leader && viewId == self.view.id) {
			self.rearmTimer()
		}

	default:
		// ignore
	}
}

func (self *Controller) onTimeout() {

	if len(self.votes) == 0 {
		// become a candidate to propose an election to our peers
		vote := &Vote{ViewId: self.view.id, PeerId: self.myId.Id}

		self.view.id++
		self.votes[self.myId] = vote
		self.broadcast(vote)
		self.state.Event("candidate")
	} else {

		// There seems to be an election already under way, lets just join it
		self.election()
	}

}

func (self *Controller) broadcast(msg proto.Message) {
	for _,peer := range self.activePeers {
		peer.Send(msg)
	}
}