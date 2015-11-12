package main

import "testing"

func TestElection(t *testing.T) {
	members := []string{"A", "B", "C", "D", "E"}
	id := "A"
	em := NewElectionManager(id, members)

	_, err := em.Current()
	if err == nil {
		t.Fail()
	}

	viewId := em.View()
	if viewId != 0 {
		t.Fatalf("unexpected viewid %d", viewId)
	}

	_,err = em.GetContender()
	if err == nil {
		t.Fail()
	}

	viewId = int64(1);
	peerId := "B"
	vote := &Vote{ViewId: &viewId, PeerId: &peerId}
	em.ProcessVote("A", vote)

	contender,err := em.GetContender()
	if err != nil {
		t.Fail()
	}

	if contender.GetPeerId() != "B" {
		t.Fatalf("unexpected contender %s", contender)
	}

	em.ProcessVote("B", vote)
	em.ProcessVote("C", vote)

	leader, err := em.Current()
	if err != nil {
		t.Fail()
	}

	if leader != "B" {
		t.Fatalf("unexpected leader %s", leader)
	}

	viewId = em.View()
	if viewId != 1 {
		t.Fatalf("unexpected viewid %d", viewId)
	}


}
