package github

import (
	"context"
	"fmt"
	"io"
	"log"

	gh "github.com/google/go-github/v66/github"
	"github.com/usebruno/mac-action-agent/internal/scheduler"
)

// RunnerStatusChecker implements scheduler.RunnerStatusChecker by querying
// GitHub's list-runners API for a runner matching the given name.
type RunnerStatusChecker struct {
	auth      AuthFunc
	newClient func(pat string) *gh.Client
	debug     *log.Logger
}

// NewRunnerStatusChecker constructs a status checker that authenticates
// each lookup using the given AuthFunc. debug receives verbose tracing;
// pass nil to disable it.
func NewRunnerStatusChecker(auth AuthFunc, debug *log.Logger) *RunnerStatusChecker {
	if debug == nil {
		debug = log.New(io.Discard, "", 0)
	}
	return &RunnerStatusChecker{
		auth: auth,
		newClient: func(pat string) *gh.Client {
			return gh.NewClient(nil).WithAuthToken(pat)
		},
		debug: debug,
	}
}

// RunnerStatus implements scheduler.RunnerStatusChecker.
func (c *RunnerStatusChecker) RunnerStatus(ctx context.Context, target scheduler.TargetRef, runnerName string) (scheduler.RunnerStatus, error) {
	if c.debug == nil {
		c.debug = log.New(io.Discard, "", 0)
	}
	pat, err := c.auth(ctx)
	if err != nil {
		return scheduler.RunnerStatus{}, fmt.Errorf("resolve PAT: %w", err)
	}
	client := c.newClient(pat)

	runners, _, err := client.Actions.ListRunners(ctx, target.Owner, target.Repo, &gh.ListRunnersOptions{
		Name: gh.String(runnerName),
	})
	if err != nil {
		return scheduler.RunnerStatus{}, fmt.Errorf("list runners for %s: %w", target.Key(), err)
	}

	for _, r := range runners.Runners {
		if r.GetName() != runnerName {
			continue
		}
		status := scheduler.RunnerStatus{
			Found:  true,
			Online: r.GetStatus() == "online",
			Busy:   r.GetBusy(),
		}
		c.debug.Printf("github: %s: runner %q status=%s busy=%v", target.Key(), runnerName, r.GetStatus(), r.GetBusy())
		return status, nil
	}

	c.debug.Printf("github: %s: runner %q not found among %d runner(s)", target.Key(), runnerName, len(runners.Runners))
	return scheduler.RunnerStatus{Found: false}, nil
}
