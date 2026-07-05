package scheduler

import (
	"reflect"
	"testing"
)

func target(owner, repo string) TargetRef {
	return TargetRef{Owner: owner, Repo: repo}
}

func TestAllocate_SpecExamples(t *testing.T) {
	tests := []struct {
		name     string
		idleVMs  int
		demands  []Demand
		expected map[string]int
	}{
		{
			name:    "both repos have demand, guarantee phase covers pool exactly",
			idleVMs: 2,
			demands: []Demand{
				{Target: target("acme", "repo1"), Weight: 2, QueuedJobs: 2},
				{Target: target("acme", "repo2"), Weight: 5, QueuedJobs: 5},
			},
			expected: map[string]int{"acme/repo1": 1, "acme/repo2": 1},
		},
		{
			name:    "repo1 has zero demand, both VMs go to repo2",
			idleVMs: 2,
			demands: []Demand{
				{Target: target("acme", "repo1"), Weight: 0, QueuedJobs: 0},
				{Target: target("acme", "repo2"), Weight: 5, QueuedJobs: 5},
			},
			expected: map[string]int{"acme/repo2": 2},
		},
		{
			name:    "three demanding repos, pool larger than demand count favors breadth",
			idleVMs: 3,
			demands: []Demand{
				{Target: target("acme", "repo1"), Weight: 2, QueuedJobs: 2},
				{Target: target("acme", "repo2"), Weight: 5, QueuedJobs: 5},
				{Target: target("acme", "repo3"), Weight: 1, QueuedJobs: 1},
			},
			expected: map[string]int{"acme/repo1": 1, "acme/repo2": 1, "acme/repo3": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Allocate(tt.idleVMs, tt.demands)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("Allocate(%d, %v) = %v, want %v", tt.idleVMs, tt.demands, got, tt.expected)
			}
		})
	}
}

func TestAllocate_ZeroPool(t *testing.T) {
	demands := []Demand{{Target: target("a", "b"), Weight: 5, QueuedJobs: 5}}
	got := Allocate(0, demands)
	if len(got) != 0 {
		t.Errorf("expected empty allocation with zero pool, got %v", got)
	}
}

func TestAllocate_NegativePool(t *testing.T) {
	demands := []Demand{{Target: target("a", "b"), Weight: 5, QueuedJobs: 5}}
	got := Allocate(-1, demands)
	if len(got) != 0 {
		t.Errorf("expected empty allocation with negative pool, got %v", got)
	}
}

func TestAllocate_ZeroDemand(t *testing.T) {
	demands := []Demand{
		{Target: target("a", "b"), Weight: 0, QueuedJobs: 0},
		{Target: target("c", "d"), Weight: 0, QueuedJobs: 0},
	}
	got := Allocate(2, demands)
	if len(got) != 0 {
		t.Errorf("expected empty allocation with zero demand, got %v", got)
	}
}

func TestAllocate_NoDemandsAtAll(t *testing.T) {
	got := Allocate(2, nil)
	if len(got) != 0 {
		t.Errorf("expected empty allocation, got %v", got)
	}
}

func TestAllocate_SingleTargetMVP(t *testing.T) {
	// Degenerate MVP case: one repo, pool of 2, only 1 job queued.
	// Clamp phase should prevent over-provisioning beyond queued work.
	demands := []Demand{{Target: target("a", "b"), Weight: 1, QueuedJobs: 1}}
	got := Allocate(2, demands)
	want := map[string]int{"a/b": 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Allocate = %v, want %v", got, want)
	}
}

func TestAllocate_SingleTargetConsumesWholePool(t *testing.T) {
	demands := []Demand{{Target: target("a", "b"), Weight: 5, QueuedJobs: 5}}
	got := Allocate(2, demands)
	want := map[string]int{"a/b": 2}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Allocate = %v, want %v", got, want)
	}
}

func TestAllocate_MoreDemandingTargetsThanPool(t *testing.T) {
	demands := []Demand{
		{Target: target("a", "1"), Weight: 3, QueuedJobs: 3},
		{Target: target("a", "2"), Weight: 3, QueuedJobs: 3},
		{Target: target("a", "3"), Weight: 3, QueuedJobs: 3},
	}
	got := Allocate(1, demands)
	// Tie-break alphabetical: a/1 wins the single slot.
	want := map[string]int{"a/1": 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Allocate = %v, want %v", got, want)
	}
}

func TestAllocate_Deterministic(t *testing.T) {
	demands := []Demand{
		{Target: target("acme", "repo1"), Weight: 2, QueuedJobs: 2},
		{Target: target("acme", "repo2"), Weight: 5, QueuedJobs: 5},
		{Target: target("acme", "repo3"), Weight: 1, QueuedJobs: 1},
	}
	first := Allocate(4, demands)
	for i := 0; i < 10; i++ {
		got := Allocate(4, demands)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("Allocate is not deterministic: run %d = %v, first = %v", i, got, first)
		}
	}
}

func TestAllocate_NeverExceedsIdleVMs(t *testing.T) {
	demands := []Demand{
		{Target: target("a", "1"), Weight: 2, QueuedJobs: 100},
		{Target: target("a", "2"), Weight: 5, QueuedJobs: 100},
		{Target: target("a", "3"), Weight: 1, QueuedJobs: 100},
	}
	for idle := 0; idle <= 10; idle++ {
		got := Allocate(idle, demands)
		sum := 0
		for _, v := range got {
			sum += v
		}
		if sum > idle {
			t.Errorf("idle=%d: allocated %d VMs, exceeds pool", idle, sum)
		}
	}
}

func TestAllocate_ClampNeverExceedsQueuedJobs(t *testing.T) {
	demands := []Demand{
		{Target: target("a", "1"), Weight: 10, QueuedJobs: 1},
		{Target: target("a", "2"), Weight: 10, QueuedJobs: 1},
	}
	got := Allocate(4, demands)
	for key, v := range got {
		if v > 1 {
			t.Errorf("target %s allocated %d VMs but only had 1 queued job", key, v)
		}
	}
}

func TestAllocate_RemainderPhaseFavorsHigherWeight(t *testing.T) {
	demands := []Demand{
		{Target: target("a", "low"), Weight: 1, QueuedJobs: 10},
		{Target: target("a", "high"), Weight: 9, QueuedJobs: 10},
	}
	got := Allocate(5, demands)
	if got["a/high"] <= got["a/low"] {
		t.Errorf("expected higher-weight target to receive more VMs, got %v", got)
	}
}
