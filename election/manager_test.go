package election

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestElection(t *testing.T) {
	members := []string{"A", "B", "C", "D", "E"}
	id := "A"
	em := NewManager(id, members)

	_, err := em.Current()
	assert.NotNil(t, err)

	viewId := em.View()
	assert.Equal(t, viewId, int64(0))

	_, _, err = em.GetContender()
	assert.NotNil(t, err)

	em.ProcessVote("A", "B", 1)

	contender, _, err := em.GetContender()
	assert.Nil(t, err)
	assert.Equal(t, contender, "B")

	em.ProcessVote("B", "B", 1)
	em.ProcessVote("C", "B", 1)

	leader, err := em.Current()
	assert.Nil(t, err)
	assert.Equal(t, leader, "B")

	viewId = em.View()
	assert.Equal(t, viewId, int64(1))

	// This should be rejected because the view is stale
	err = em.ProcessVote("D", "B", 1)
	assert.NotNil(t, err)
}
