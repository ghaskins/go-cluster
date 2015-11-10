package main

import (
	"math"
)

func ComputeQuorumThreshold(memberCount int) int {
	return int(math.Ceil(float64(memberCount) / 2.0))
}
