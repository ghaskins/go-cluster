package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestElection(t *testing.T) {
	members := []string{"A", "B", "C", "D", "E"}
	id := "A"
	em := NewElectionManager(id, members)

	_, err := em.Current()
	assert.NotNil(t, err)

	viewId := em.View()
	assert.Equal(t, viewId, int64(0))

	_, err = em.GetContender()
	assert.NotNil(t, err)

	viewId = int64(1)
	peerId := "B"
	vote := &Vote{ViewId: &viewId, PeerId: &peerId}
	em.ProcessVote("A", vote)

	contender, err := em.GetContender()
	assert.Nil(t, err)
	assert.Equal(t, contender.GetPeerId(), "B")

	em.ProcessVote("B", vote)
	em.ProcessVote("C", vote)

	leader, err := em.Current()
	assert.Nil(t, err)
	assert.Equal(t, leader, "B")

	viewId = em.View()
	assert.Equal(t, viewId, int64(1))

	// This should be rejected because the view is stale
	err = em.ProcessVote("D", vote)
	assert.NotNil(t, err)
}
