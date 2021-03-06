package main

import (
	"crypto/rand"
	"fmt"
	"github.com/ghaskins/go-cluster/election"
	"github.com/ghaskins/go-cluster/pb"
	"github.com/ghaskins/go-cluster/util"
	"github.com/golang/protobuf/proto"
	"github.com/looplab/fsm"
	"math/big"
	"time"
)

type Controller struct {
	state           *fsm.FSM
	peers           IdentityMap
	connMgr         *ConnectionManager
	myId            string
	activePeers     map[string]*Peer
	quorumThreshold int
	timer           *time.Timer
	pulse           *time.Ticker
	electionManager *election.Manager
	minTmo          int64
	maxTmo          int64
}

func NewController(_id string, _peers IdentityMap, _connMgr *ConnectionManager) *Controller {

	var members []string

	for _, peer := range _peers {
		members = append(members, peer.Id)
	}

	self := &Controller{
		peers:           _peers,
		connMgr:         _connMgr,
		myId:            _id,
		activePeers:     make(map[string]*Peer),
		quorumThreshold: util.ComputeQuorumThreshold(len(_peers)) - 1, // We don't include ourselves
		timer:           time.NewTimer(0),
		pulse:           time.NewTicker(1),
		electionManager: election.NewManager(_id, members),
		minTmo:          500,
		maxTmo:          1000,
	}

	<-self.timer.C // drain the initial event
	self.pulse.Stop()
	<-self.pulse.C // drain the initial event

	self.state = fsm.NewFSM(
		"convening",
		fsm.Events{
			{Name: "quorum", Src: []string{"convening"}, Dst: "initializing"},
			{Name: "quorum-lost", Src: []string{"initializing", "electing", "electing-restart", "following", "leading"}, Dst: "convening"},
			{Name: "elected-self", Src: []string{"initializing", "electing"}, Dst: "leading"},
			{Name: "elected-other", Src: []string{"initializing", "electing"}, Dst: "following"},
			{Name: "timeout", Src: []string{"initializing", "following", "electing"}, Dst: "electing"},
			{Name: "election", Src: []string{"following", "leading"}, Dst: "electing"},
			{Name: "heartbeat", Src: []string{"following"}, Dst: "following"},
		},
		fsm.Callbacks{
			"convening":          func(e *fsm.Event) { self.onConvening() },
			"enter_initializing": func(e *fsm.Event) { self.onInitializing() },
			"leave_initializing": func(e *fsm.Event) { self.timer.Stop() },
			"enter_following":    func(e *fsm.Event) { self.onEnterFollowing() },
			"leave_following":    func(e *fsm.Event) { self.onLeaveFollowing() },
			"enter_electing":     func(e *fsm.Event) { self.onElecting() },
			"leave_electing":     func(e *fsm.Event) { self.timer.Stop() },
			"enter_leading":      func(e *fsm.Event) { self.onEnterLeading() },
			"leave_leading":      func(e *fsm.Event) { self.onLeaveLeading() },
			"heartbeat":          func(e *fsm.Event) { self.onHeartBeat(e.Args[0].(string), e.Args[1].(int64)) },
			"before_timeout":     func(e *fsm.Event) { self.onTimeout() },
		},
	)

	return self
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
		case conn := <-self.connMgr.C:
			fmt.Printf("new connection from %s\n", conn.Id.Id)

			if _, ok := self.activePeers[conn.Id.Id]; ok {
				fmt.Printf("client is already connected")
				continue
			}

			peer := &Peer{conn: conn, rxChannel: &messageEvents, disconnectChannel: &disconnectionEvents}
			self.activePeers[conn.Id.Id] = peer
			peer.Run()

			self.state.Event("connection", conn.Id.Id)

			if len(self.activePeers) >= self.quorumThreshold {
				self.state.Event("quorum")
			}

			// Update the peer with an unsolicited vote if we already have an opinion on who is leader
			switch self.state.Current() {
			case "leading":
				fallthrough
			case "following":
				leader, err := self.electionManager.Current()
				viewId := self.electionManager.View()
				if err == nil {
					msg := &pb.Vote{
						ViewId: &viewId,
						PeerId: &leader,
					}
					peer.Send(msg)
				}
			default:
				contender, viewId, err := self.electionManager.GetContender()
				if err == nil {
					msg := &pb.Vote{
						ViewId: &viewId,
						PeerId: &contender,
					}
					peer.Send(msg)
				}
			}

		//---------------------------------------------------------
		// message arrival
		//---------------------------------------------------------
		case _msg := <-messageEvents:
			switch _msg.Payload.(type) {
			case *pb.Heartbeat:
				msg := _msg.Payload.(*pb.Heartbeat)
				self.state.Event("heartbeat", _msg.From.Id(), msg.GetViewId())
			case *pb.Vote:
				msg := _msg.Payload.(*pb.Vote)
				self.onVote(_msg.From.Id(), msg.GetPeerId(), msg.GetViewId())
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
				viewId := self.electionManager.View()
				self.broadcast(&pb.Heartbeat{ViewId: &viewId})
			}

		//---------------------------------------------------------
		// disconnects
		//---------------------------------------------------------
		case peerId := <-disconnectionEvents:
			fmt.Printf("lost connection from %s\n", peerId)
			if len(self.activePeers) <= self.quorumThreshold {
				self.state.Event("quorum-lost")
			}
			delete(self.activePeers, peerId)
			self.electionManager.Invalidate(peerId)
			self.connMgr.Dial(peerId)
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

func printSeparator() {
	fmt.Println("---------------------------------------------------")
}

func (self *Controller) castBallot(peerId string, viewId int64) {
	fmt.Printf("broadcasting vote for %s in view %d\n", peerId, viewId)
	err := self.electionManager.ProcessVote(self.myId, peerId, viewId)
	if err != nil {
		panic(err)
	}

	msg := &pb.Vote{
		ViewId: &viewId,
		PeerId: &peerId,
	}
	self.broadcast(msg)
}

func (self *Controller) castSelfBallot() {
	// Vote for ourselves if there isn't a current contender
	viewId := self.electionManager.View()

	self.castBallot(self.myId, viewId)
}

func (self *Controller) onVote(from, peerId string, viewId int64) {
	allow := false

	switch self.state.Current() {
	case "convening":
		fallthrough
	case "initializing":
		allow = true // Allow any vote through in convening/initializing state
	case "electing":
		// Only allow votes for the current view through
		if viewId == self.electionManager.View() {
			allow = true
		}
	case "following":
		fallthrough
	case "leading":
		// Only allow votes for the next view through
		if viewId == self.electionManager.View()+1 {
			allow = true
		}
	}

	if !allow {
		fmt.Printf("Dropping vote from %s for %s:%d in state %s\n", from, peerId, viewId, self.state.Current())
		return
	}

	err := self.electionManager.ProcessVote(from, peerId, viewId)
	if err != nil {
		fmt.Printf("Dropping vote from %s: %s\n", from, err.Error())
	}
}

func (self *Controller) onConvening() {
	fmt.Printf("onConvening\n")
}

func (self *Controller) onInitializing() {
	fmt.Printf("onInitializing\n")
	self.rearmTimeout()
}

func (self *Controller) onHeartBeat(from string, viewId int64) {
	leader, err := self.electionManager.Current()
	if err != nil {
		panic(err)
	}

	// Only pet the watchdog if the HB originated from the node we believe to be the leader
	if from == leader && viewId == self.electionManager.View() {
		self.rearmTimeout()
	}
}

func (self *Controller) onTimeout() {

	fmt.Printf("onTimeout\n")

	self.rearmTimeout()
}

func (self *Controller) onElecting() {
	fmt.Printf("onElecting\n")

	contender, view, err := self.electionManager.GetContender()
	if err != nil {
		// Vote for ourselves if there isn't a current contender
		self.castSelfBallot()
	} else {
		self.castBallot(contender, view)
	}

	self.rearmTimeout()
}

func (self *Controller) onEnterFollowing() {
	self.rearmTimeout()
	leader, err := self.electionManager.Current()
	if err != nil {
		panic(err)
	}

	printSeparator()
	fmt.Printf("VIEW %d: FOLLOWING %s\n", self.electionManager.View(), leader)
	printSeparator()
}

func (self *Controller) onLeaveFollowing() {
	self.electionManager.NextView()
	self.timer.Stop()
}

func (self *Controller) onEnterLeading() {
	printSeparator()
	fmt.Printf("VIEW %d: LEADING\n", self.electionManager.View())
	printSeparator()

	self.pulse = time.NewTicker(time.Millisecond * time.Duration(self.minTmo/2))
}

func (self *Controller) onLeaveLeading() {
	self.electionManager.NextView()
	self.pulse.Stop()
}

func (self *Controller) broadcast(msg proto.Message) {
	for _, peer := range self.activePeers {
		peer.Send(msg)
	}
}
