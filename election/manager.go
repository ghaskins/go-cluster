package election

import (
	"errors"
	"fmt"
	"github.com/ghaskins/go-cluster/util"
	"github.com/looplab/fsm"
)

type Vote struct {
	viewId int64
	peerId string
}

type Votes map[string]Vote

type Manager struct {
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

func NewManager(_myId string, _members []string) *Manager {
	self := &Manager{
		myId:      _myId,
		members:   _members,
		votes:     make(Votes),
		threshold: util.ComputeQuorumThreshold(len(_members)),
		C:         make(chan bool, 100),
	}

	self.state = fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "quorum", Src: []string{"idle", "elected"}, Dst: "electing"},
			{Name: "complete", Src: []string{"electing"}, Dst: "elected"},
			{Name: "next", Src: []string{"elected"}, Dst: "idle"},
		},
		fsm.Callbacks{
			"electing": func(e *fsm.Event) { self.onElecting() },
			"enter_elected":  func(e *fsm.Event) { self.onElected(e.Args[0].(string), e.Args[1].(int64)) },
			"leave_elected":  func(e *fsm.Event) { self.view++ },
		},
	)

	fmt.Printf("EM: Initializing with %d members and a quorum threshold %d\n", len(self.members), self.threshold)

	return self
}

func (self *Manager) Current() (string, error) {
	switch self.state.Current() {
	case "elected":
		return self.leader, nil
	default:
		return "", errors.New("leader unknown")
	}

}

func (self *Manager) View() int64 {
	return self.view
}

func (self *Manager) Invalidate(member string) {
	delete(self.votes, member)
	if self.state.Current() == "elected" && member == self.leader {
		self.state.Event("leader-lost")
	}
}

func (self *Manager) VoteCount() int {
	return len(self.votes)
}

func (self *Manager) GetContender() (string, int64, error) {
	if len(self.votes) == 0 {
		return "", 0, errors.New("no candidates present")
	}

	results := make(map[string]int)

	var max int

	// Accumulate all the votes by peer, and make note of the largest
	for _, vote := range self.votes {
		peerId := vote.peerId
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
		return self.first.peerId, self.first.viewId, nil
	} else {
		// Now go find the first entry with the same max
		for peerId, votes := range results {
			if votes == max {
				vote := self.votes[peerId]
				return vote.peerId, vote.viewId, nil
			}
		}
	}

	return "", 0, errors.New("no candidates computed")

}

func (self *Manager) ProcessVote(from, peerId string, viewId int64) error {

	fmt.Printf("EM: vote for %s in view %d from %s\n", peerId, viewId, from)

	if viewId < self.view || (self.state.Current() == "elected" && viewId == self.view) {
		return errors.New("ignoring stale vote")
	}

	prevCount := len(self.votes)

	vote := &Vote{viewId: viewId, peerId: peerId}
	self.votes[from] = *vote
	if prevCount == 0 {
		self.first = vote // This gives extra weight to the first received vote
	}

	currCount := len(self.votes)

	// Our criteria for proposal-quorum is threshold - 1 because we don't include ourselves
	if prevCount != currCount && currCount == (self.threshold - 1) {
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
		peerId := vote.peerId
		viewId := vote.viewId
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

func (self *Manager) onElecting() {
	fmt.Printf("EM: Begin Election\n")
	self.C <- false
}

func (self *Manager) onElected(leader string, view int64) {
	fmt.Printf("EM: Election Complete, new leader = %s\n", leader)
	self.leader = leader
	self.view = view
	self.votes = make(Votes) // clear any outstanding votes
	self.first = nil
	self.C <- true // notify our observers
}

func (self *Manager) NextView() {
	self.state.Event("next")
}
