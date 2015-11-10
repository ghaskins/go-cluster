package main

import (
	"github.com/looplab/fsm"
	"github.com/golang/protobuf/proto"
)

type Votes map[string]*Vote


type ElectionManager struct {
	state             fsm.FSM
	myId              string
	members           []string
	votes             Votes
	leader            string
	threshold         int
	C                 chan bool
}

func NewElectionManager(_myId string, _members []string) {
	self := &ElectionManager{
		myId: _myId,
		members: _members,
		threshold: ComputeQuorumThreshold(len(_members)),
		C: make(chan bool),
	}

	self.state = fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "quorum", Src: []string{"idle", "resolved"},  Dst: "electing"},
			{Name: "complete", Src: []string{"electing"},        Dst: "elected"},

		},
		fsm.Callbacks{
			"electing": func(e *fsm.Event) { self.C <- false },
			"elected": func(e *fsm.Event) { self.onElected(e.Args[0]) },
		},
	)
}

func (self *ElectionManager) Current() string {
	return self.leader
}

func (self *ElectionManager) Invalidate(member string) {
	delete(self.votes, member)
}

func (self *ElectionManager) Vote(from string, vote *Vote) {
	prevCount := len(self.votes)
	isFirst := prevCount == 0

	self.votes[from] = vote
	if isFirst && from != self.myId {
		self.votes[self.myId] = vote // This gives extra weight to the first received vote
	}

	currCount := len(self.votes)

	if prevCount != currCount && currCount == self.threshold {
		self.state.Event("quorum")
	}

	results := make(map[string]int)

	// We will choose the first entry with enough accumulated votes to exceed quorum
	for _,peerId := range self.votes {
		result := &results[peerId]
		result++;
		if result >= self.threshold {
			self.state.Event("complete", peerId)
			continue
		}
	}
}

func (self *ElectionManager) onElected(leader string) {
	self.leader = leader
	self.votes = make(Votes) // clear any outstanding votes
	self.C <- true // notify our observers
}
