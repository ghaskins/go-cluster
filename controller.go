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
	leader *Identity
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
}

func NewController(_id *Identity, _peers IdentityMap) *Controller {
	self := &Controller{
		connectionEvents: make(chan *Connection),
		peers: _peers,
		myId: _id,
		activePeers: make(map[string]Peer),
		quorumThreshold: int(math.Ceil(len(_peers) / 2.0)) - 1, // We don't include ourselves
		timer: make(chan time.Time),
		votes: make(Votes),
	}


	self.state = fsm.NewFSM(
		"sensing",
		fsm.Events{
			{Name: "member-quorum-acquired", Src: []string{"sensing"},  Dst: "follower"},
			{Name: "member-quorum-lost",   						   Dst: "sensing"},
			{Name: "candidate",       Src: []string{"follower"}, Dst: "candidate"},
			{Name: "ping",          Src: []string{"follower", "candidate"}, Dst: "follower"},
			{Name: "ping",          Src: []string{"leader"}, Dst: "candidate"},
			{Name: "elected",       Src: []string{"candidate"}, Dst: "leader"},
			{Name: "yield",         Src: []string{"candidate"}, Dst: "follower"},
			{Name: "vote",          Src: []string{"leader"}, Dst: "candidate"},
		},
		fsm.Callbacks{
			"enter_follower": func(e *fsm.Event) { self.rearmTimer() },
			"leave_follower": func(e *fsm.Event) { self.timer.Stop() },

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
				self.state.Event("member-quorum-acquired")
			}

			// Update the peer with an unsolicited vote if we already have an opinion on who is leader
			if self.view.leader != nil {
				msg := &Vote{
					ViewId: self.view.id,
					PeerId: self.view.leader.Id,
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
				self.onVote(_msg.From.Id(), msg)
			}

		//---------------------------------------------------------
		// timeouts
		//---------------------------------------------------------
		case _ := <-self.timer:
			self.onTimeout()

		//---------------------------------------------------------
		// disconnects
		//---------------------------------------------------------
		case peerId := <-disconnectionEvents:
			fmt.Printf("lost connection from %s\n", peerId)
			if (len(self.activePeers) == self.quorumThreshold) {
				self.state.Event("member-quorum-lost")
			}
			delete(self.activePeers, peerId)
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
	case "follower":

		// Only pet the watchdog if the HB originated from the node we believe to be the leader
		if (from == self.view.leader.Id && viewId == self.view.id) {
			self.rearmTimer()
		}

	default:
		// ignore
	}
}

func (self *Controller) election() {
	results := make(map[string]int)

	// First aggregate the votes by peerId
	for _,peerId := range self.votes {
		results[peerId]++;
	}

	// We will vote for the entry with the highest accumulated votes

	self.votes = make(Votes) // clear any outstanding votes
}

func (self *Controller) onVote(from string, vote *Vote) {

	prevCount := len(self.votes)
	isFirst := prevCount == 0

	self.votes[from] = vote
	if isFirst {
		self.votes[self.myId] = vote // This gives extra weight to the first received vote
	}

	currCount := len(self.votes)

	// +2 because we do not want to include our local vote (added above) and we want it to be greater than..
	if prevCount != currCount && currCount == (self.quorumThreshold + 2) {
		// Hmmm, enough nodes are complaining about the leader that we should force a re-election even
		// though the timeout has not affected us yet

		self.state.Event("election-quorum")
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