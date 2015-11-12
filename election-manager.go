package main

import (
	"errors"
	"fmt"
	"github.com/looplab/fsm"
)

type Votes map[string]*Vote

type ElectionManager struct {
	state     *fsm.FSM
	myId      string
	members   []string
	votes     Votes
	first     *Vote
	leader    string
	view      int64
	threshold int
	C         chan bool
}

func NewElectionManager(_myId string, _members []string) *ElectionManager {
	self := &ElectionManager{
		myId:      _myId,
		members:   _members,
		votes:     make(Votes),
		threshold: ComputeQuorumThreshold(len(_members)),
		C:         make(chan bool, 100),
	}

	self.state = fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "quorum", Src: []string{"idle", "resolved"}, Dst: "electing"},
			{Name: "complete", Src: []string{"electing"}, Dst: "elected"},
			{Name: "leader-lost", Src: []string{"elected"}, Dst: "idle"},
		},
		fsm.Callbacks{
			"electing": func(e *fsm.Event) { self.onElecting() },
			"elected":  func(e *fsm.Event) { self.onElected(e.Args[0].(string), e.Args[1].(int64)) },
		},
	)

	fmt.Printf("EM: Initializing with %d members and a quorum threshold %d\n", len(self.members), self.threshold)

	return self
}

func (self *ElectionManager) Current() (string, error) {
	switch self.state.Current() {
	case "elected":
		return self.leader, nil
	default:
		return "", errors.New("leader unknown")
	}

}

func (self *ElectionManager) View() int64 {
	return self.view
}

func (self *ElectionManager) Invalidate(member string) {
	delete(self.votes, member)
	if self.state.Current() == "elected" && member == self.leader {
		self.state.Event("leader-lost")
	}
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

	if max == 1 {
		// If we don't have any one peer with more than one vote, the first vote received
		// has the most weight
		return self.first, nil
	} else {
		// Now go find the first entry with the same max
		for peerId, votes := range results {
			if votes == max {
				return self.votes[peerId], nil
			}
		}
	}

	return nil, errors.New("no candidates computed")

}

func (self *ElectionManager) ProcessVote(from string, vote *Vote) error {

	fmt.Printf("vote for %s from %s\n", vote.GetPeerId(), from)

	if vote.GetViewId() < self.view {
		return errors.New("ignoring stale vote")
	}

	prevCount := len(self.votes)

	self.votes[from] = vote
	if prevCount == 0 {
		self.first = vote // This gives extra weight to the first received vote
	}

	currCount := len(self.votes)

	if prevCount != currCount && currCount == self.threshold {
		self.state.Event("quorum")
	}

	type Result struct {
		votes     int64
		maxViewId int64
	}

	results := make(map[string]*Result)

	// We will choose the first entry with enough accumulated votes to exceed quorum.  There should
	// only be one
	for _, vote := range self.votes {
		peerId := vote.GetPeerId()
		viewId := vote.GetViewId()
		result, ok := results[peerId]
		if !ok {
			result = &Result{votes: 1}
			results[peerId] = result
		} else {
			result.votes++
		}

		if viewId > result.maxViewId {
			result.maxViewId = viewId
		}

		if result.votes >= int64(self.threshold) {
			self.state.Event("complete", peerId, result.maxViewId)
			continue
		}
	}

	return nil
}

func (self *ElectionManager) onElecting() {
	fmt.Printf("EM: Begin Election\n")
	self.C <- false
}

func (self *ElectionManager) onElected(leader string, view int64) {
	fmt.Printf("EM: Election Complete, new leader = %s\n", leader)
	self.leader = leader
	self.view = view
	self.votes = make(Votes) // clear any outstanding votes
	self.first = nil
	self.C <- true // notify our observers
}

func (self *ElectionManager) NextView() {
	self.view++
}
