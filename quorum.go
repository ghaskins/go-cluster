package main

import (
	"math"
)

func ComputeQuorumThreshold(memberCount int) int {
	return int(math.Ceil(memberCount / 2.0))
}