package scheduler

import "sort"

// Demand is one target's weighted request for VMs in a given tick.
type Demand struct {
	Target     TargetRef
	Weight     float64
	QueuedJobs int
}

// Allocate decides how many idle VMs to assign to each demanding target.
//
// Algorithm ("at-least-one-if-demand, then largest-remainder apportionment"):
//  1. Guarantee phase: every target with Weight > 0 gets 1 VM, up to idleVMs
//     capacity, ordered by weight descending (ties broken alphabetically by
//     target key for determinism). This prevents a low-demand target from
//     being starved by a high-demand one.
//  2. Remainder phase: any leftover idle VMs are distributed proportionally
//     to weight across all demanding targets using the Hamilton /
//     largest-remainder method, so higher-weight targets still receive extra
//     capacity ahead of lower-weight ones.
//  3. Clamp: no target is ever allocated more VMs than it has QueuedJobs.
func Allocate(idleVMs int, demands []Demand) map[string]int {
	result := map[string]int{}
	if idleVMs <= 0 {
		return result
	}

	demanding := make([]Demand, 0, len(demands))
	for _, d := range demands {
		if d.Weight > 0 {
			demanding = append(demanding, d)
		}
	}
	if len(demanding) == 0 {
		return result
	}

	sort.Slice(demanding, func(i, j int) bool {
		if demanding[i].Weight != demanding[j].Weight {
			return demanding[i].Weight > demanding[j].Weight
		}
		return demanding[i].Target.Key() < demanding[j].Target.Key()
	})

	// Guarantee phase.
	k := len(demanding)
	if idleVMs < k {
		k = idleVMs
	}
	for i := 0; i < k; i++ {
		result[demanding[i].Target.Key()] = 1
	}

	// Remainder phase.
	remaining := idleVMs - k
	if remaining > 0 {
		totalWeight := 0.0
		for _, d := range demanding {
			totalWeight += d.Weight
		}

		type share struct {
			key       string
			floor     int
			remainder float64
		}
		shares := make([]share, len(demanding))
		distributed := 0
		for i, d := range demanding {
			exact := float64(remaining) * d.Weight / totalWeight
			floor := int(exact)
			shares[i] = share{key: d.Target.Key(), floor: floor, remainder: exact - float64(floor)}
			distributed += floor
		}

		leftover := remaining - distributed
		sort.Slice(shares, func(i, j int) bool {
			if shares[i].remainder != shares[j].remainder {
				return shares[i].remainder > shares[j].remainder
			}
			return shares[i].key < shares[j].key
		})
		for i := range shares {
			if i < leftover {
				shares[i].floor++
			}
		}
		for _, s := range shares {
			result[s.key] += s.floor
		}
	}

	// Clamp phase: never allocate more than a target has queued jobs.
	for _, d := range demanding {
		if v, ok := result[d.Target.Key()]; ok && v > d.QueuedJobs {
			result[d.Target.Key()] = d.QueuedJobs
		}
	}
	for k, v := range result {
		if v <= 0 {
			delete(result, k)
		}
	}

	return result
}
