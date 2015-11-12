package main

import (
	"crypto/rand"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/looplab/fsm"
	"math/big"
	"time"
)

type Controller struct {
	state            *fsm.FSM
	connectionEvents chan *Connection
	peers            IdentityMap
	myId             string
	activePeers      map[string]*Peer
	quorumThreshold  int
	viewId           int64
	timer            *time.Timer
	pulse            *time.Ticker
	electionManager  *ElectionManager
	minTmo           int64
	maxTmo           int64
}

func NewController(_id string, _peers IdentityMap) *Controller {

	var members []string

	for _, peer := range _peers {
		members = append(members, peer.Id)
	}

	self := &Controller{
		connectionEvents: make(chan *Connection, 100),
		peers:            _peers,
		myId:             _id,
		activePeers:      make(map[string]*Peer),
		quorumThreshold:  ComputeQuorumThreshold(len(_peers)) - 1, // We don't include ourselves
		timer:            time.NewTimer(0),
		pulse:            time.NewTicker(1),
		electionManager:  NewElectionManager(_id, members),
		minTmo:           500,
		maxTmo:           1000,
	}

	<-self.timer.C // drain the initial event
	self.pulse.Stop()
	<-self.pulse.C // drain the initial event

	self.state = fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "connection", Src: []string{"idle"}, Dst: "initializing"},
			{Name: "elected-self", Src: []string{"initializing", "electing"}, Dst: "leading"},
			{Name: "elected-other", Src: []string{"initializing", "electing"}, Dst: "following"},
			{Name: "timeout", Src: []string{"initializing", "following"}, Dst: "electing"},
			{Name: "election", Src: []string{"following", "leading"}, Dst: "electing"},
			{Name: "heartbeat", Src: []string{"following"}, Dst: "following"},
		},
		fsm.Callbacks{
			"enter_initializing": func(e *fsm.Event) { self.rearmTimeout() },
			"leave_initializing": func(e *fsm.Event) { self.timer.Stop() },
			"enter_following":    func(e *fsm.Event) { self.onFollowing() },
			"leave_following":    func(e *fsm.Event) { self.timer.Stop() },
			"enter_leading":      func(e *fsm.Event) { self.onLeading() },
			"leave_leading":      func(e *fsm.Event) { self.pulse.Stop() },
			"heartbeat":          func(e *fsm.Event) { self.onHeartBeat(e.Args[0].(string), e.Args[1].(int64)) },
			"electing":           func(e *fsm.Event) { self.onElecting() },
			"before_timeout":     func(e *fsm.Event) { self.onTimeout() },
		},
	)

	return self
}

func (self *Controller) Connect(conn *Connection) {
	self.connectionEvents <- conn
}

func (self *Controller) Run() {

	disconnectionEvents := make(DisconnectChannel, 100)
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
			self.activePeers[conn.Id.Id] = peer
			peer.Run()

			self.state.Event("connection", conn.Id.Id)

			// Update the peer with an unsolicited vote if we already have an opinion on who is leader
			switch self.state.Current() {
			case "leading":
				fallthrough
			case "following":
				leader, err := self.electionManager.Current()
				if err == nil {
					msg := &Vote{
						ViewId: &self.viewId,
						PeerId: &leader,
					}
					peer.Send(msg)
				}
			case "electing":
				contender, err := self.electionManager.GetContender()
				if err == nil {
					peer.Send(contender)
				}
			}

		//---------------------------------------------------------
		// message arrival
		//---------------------------------------------------------
		case _msg := <-messageEvents:
			switch _msg.Payload.(type) {
			case *Heartbeat:
				msg := _msg.Payload.(*Heartbeat)
				self.state.Event("heartbeat", _msg.From.Id(), msg.GetViewId())
			case *Vote:
				msg := _msg.Payload.(*Vote)
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
					panic(err)
				}

				if leader == self.myId {
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
		case _ = <-self.timer.C:
			self.state.Event("timeout")

		//---------------------------------------------------------
		// pulse ticker
		//---------------------------------------------------------
		case _ = <-self.pulse.C:
			if self.state.Current() == "leading" {
				self.broadcast(&Heartbeat{ViewId: &self.viewId})
			}

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

func (self *Controller) rearmTimeout() {
	offset, err := rand.Int(rand.Reader, big.NewInt(self.maxTmo-self.minTmo))
	if err != nil {
		panic("bad return from rand.Int")
	}

	self.rearmTimer(self.minTmo + offset.Int64())
}

func (self *Controller) rearmTimer(tmo int64) {
	//fmt.Printf("(re)arming timer with %vms\n", tmo)
	self.timer.Reset(time.Millisecond * time.Duration(tmo))
}

func (self *Controller) onHeartBeat(from string, viewId int64) {
	leader, err := self.electionManager.Current()
	if err != nil {
		panic(err)
	}

	// Only pet the watchdog if the HB originated from the node we believe to be the leader
	if from == leader && viewId == self.viewId {
		self.rearmTimeout()
	}
}

func (self *Controller) onTimeout() {

	fmt.Printf("onTimeout\n")

	self.rearmTimeout()
}

func (self *Controller) onElecting() {
	fmt.Printf("onElecting\n")

	vote, err := self.electionManager.GetContender()
	if err != nil {
		// Vote for ourselves if there isn't a current contender
		nextViewId := self.viewId + 1
		vote = &Vote{ViewId: &nextViewId, PeerId: &self.myId}
	}

	self.electionManager.ProcessVote(self.myId, vote)

	fmt.Printf("broadcasting vote for %s\n", vote.GetPeerId())
	self.broadcast(vote)
}

func (self *Controller) onFollowing() {
	self.rearmTimeout()
	leader, err := self.electionManager.Current()
	if err != nil {
		panic(err)
	}

	fmt.Printf("following %s\n", leader)
}

func (self *Controller) onLeading() {
	fmt.Printf("we are the leader\n")

	self.pulse = time.NewTicker(time.Millisecond * time.Duration(self.minTmo/2))
}

func (self *Controller) broadcast(msg proto.Message) {
	for _, peer := range self.activePeers {
		peer.Send(msg)
	}
}
