package github

import (
	"context"
	"fmt"
	"io"
	"log"

	gh "github.com/google/go-github/v66/github"
	"github.com/barelyhuman/mac-runners-manager/internal/scheduler"
)

// StaleRunnerCleaner can force-delete a runner registration from GitHub when
// its JIT config has expired and we need to mint a fresh one.
type StaleRunnerCleaner struct {
	auth      AuthFunc
	newClient func(pat string) *gh.Client
	debug     *log.Logger
}

// NewStaleRunnerCleaner constructs a cleaner that authenticates using the
// given AuthFunc.
func NewStaleRunnerCleaner(auth AuthFunc, debug *log.Logger) *StaleRunnerCleaner {
	if debug == nil {
		debug = log.New(io.Discard, "", 0)
	}
	return &StaleRunnerCleaner{
		auth: auth,
		newClient: func(pat string) *gh.Client {
			return gh.NewClient(nil).WithAuthToken(pat)
		},
		debug: debug,
	}
}

// DeleteRunnerByName removes a self-hosted runner from a repository by name.
// It first resolves the runner ID via ListRunners, then DELETEs it. If the
// runner is not found this is a no-op (no error).
func (c *StaleRunnerCleaner) DeleteRunnerByName(ctx context.Context, target scheduler.TargetRef, runnerName string) error {
	pat, err := c.auth(ctx)
	if err != nil {
		return fmt.Errorf("resolve PAT: %w", err)
	}
	client := c.newClient(pat)

	runnerID, err := c.resolveRunnerID(ctx, client, target, runnerName)
	if err != nil {
		return fmt.Errorf("resolve runner ID for %s: %w", runnerName, err)
	}
	if runnerID == 0 {
		c.debug.Printf("github: no stale runner %q found in %s, nothing to delete", runnerName, target.Key())
		return nil
	}

	resp, err := client.Actions.RemoveRunner(ctx, target.Owner, target.Repo, runnerID)
	if err != nil {
		// A 404 here means the runner was already removed between the list and delete.
		if resp != nil && resp.StatusCode == 404 {
			c.debug.Printf("github: runner %q was already deleted (404)", runnerName)
			return nil
		}
		return fmt.Errorf("remove runner %d (%s): %w", runnerID, runnerName, err)
	}

	c.debug.Printf("github: deleted stale runner %q (id=%d) from %s", runnerName, runnerID, target.Key())
	return nil
}

// resolveRunnerID lists runners for the target repo and returns the ID of
// the runner whose name matches runnerName, or 0 if not found.
func (c *StaleRunnerCleaner) resolveRunnerID(ctx context.Context, client *gh.Client, target scheduler.TargetRef, runnerName string) (int64, error) {
	runners, _, err := client.Actions.ListRunners(ctx, target.Owner, target.Repo, &gh.ListRunnersOptions{
		Name: gh.String(runnerName),
	})
	if err != nil {
		return 0, fmt.Errorf("list runners for %s: %w", target.Key(), err)
	}

	for _, r := range runners.Runners {
		if r.GetName() == runnerName {
			return r.GetID(), nil
		}
	}
	return 0, nil
}
