package main

import (
	"errors"
	"github.com/looplab/fsm"
)

type Votes map[string]*Vote

type ElectionManager struct {
	state     *fsm.FSM
	myId      string
	members   []string
	votes     Votes
	leader    string
	threshold int
	C         chan bool
}

func NewElectionManager(_myId string, _members []string) *ElectionManager {
	self := &ElectionManager{
		myId:      _myId,
		members:   _members,
		votes:     make(Votes),
		threshold: ComputeQuorumThreshold(len(_members)),
		C:         make(chan bool),
	}

	self.state = fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "quorum", Src: []string{"idle", "resolved"}, Dst: "electing"},
			{Name: "complete", Src: []string{"electing"}, Dst: "elected"},
		},
		fsm.Callbacks{
			"electing": func(e *fsm.Event) { self.C <- false },
			"elected":  func(e *fsm.Event) { self.onElected(e.Args[0].(string)) },
		},
	)

	return self
}

func (self *ElectionManager) Current() (string, error) {
	switch self.state.Current() {
	case "complete":
		return self.leader, nil
	default:
		return "", errors.New("leader unknown")
	}

}

func (self *ElectionManager) Invalidate(member string) {
	delete(self.votes, member)
}

func (self *ElectionManager) VoteCount() int {
	return len(self.votes)
}

func (self *ElectionManager) GetContender() (*Vote, error) {
	if len(self.votes) == 0 {
		return nil, errors.New("no candidates present")
	}

	results := make(map[string]int)

	var max int

	// Accumulate all the votes by peer, and make note of the largest
	for _, vote := range self.votes {
		peerId := vote.GetPeerId()
		result := results[peerId]
		result++
		results[peerId] = result

		if result > max {
			max = result
		}
	}

	// Now go find the first entry with the same max
	for peerId, votes := range results {
		if votes == max {
			return self.votes[peerId], nil
		}
	}

	return nil, errors.New("no candidates computed")

}

func (self *ElectionManager) ProcessVote(from string, vote *Vote) {
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

	// We will choose the first entry with enough accumulated votes to exceed quorum.  There should
	// only be one
	for _, vote := range self.votes {
		peerId := vote.GetPeerId()
		result := results[peerId]
		result++
		results[peerId] = result
		if result >= self.threshold {
			self.state.Event("complete", peerId)
			continue
		}
	}
}

func (self *ElectionManager) onElected(leader string) {
	self.leader = leader
	self.votes = make(Votes) // clear any outstanding votes
	self.C <- true           // notify our observers
}
