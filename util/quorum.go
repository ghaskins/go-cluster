package util

import (
	"math"
)

func ComputeQuorumThreshold(memberCount int) int {
	return int(math.Ceil(float64(memberCount) * .5000000001)) // something *just* north of 50%
}
