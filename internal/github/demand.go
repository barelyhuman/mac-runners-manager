// Package github implements the GitHub-facing pieces of mac-action-agent:
// polling for queued jobs and minting JIT runner registration configs.
package github

import (
	"context"
	"fmt"
	"io"
	"log"

	gh "github.com/google/go-github/v66/github"
	"github.com/usebruno/mac-action-agent/internal/scheduler"
)

// AuthFunc resolves the GitHub PAT to use for API calls. Implemented by
// internal/jsconfig via the user-authored config.js auth() function.
type AuthFunc func(ctx context.Context) (string, error)

// PollingDemandSource implements scheduler.DemandSource by polling the
// GitHub REST API for queued workflow jobs on each configured target.
type PollingDemandSource struct {
	auth      AuthFunc
	newClient func(pat string) *gh.Client
	debug     *log.Logger
}

// NewPollingDemandSource constructs a demand source that authenticates each
// poll using the given AuthFunc. debug receives verbose tracing of queued
// job/label matching; pass nil to disable it.
func NewPollingDemandSource(auth AuthFunc, debug *log.Logger) *PollingDemandSource {
	if debug == nil {
		debug = log.New(io.Discard, "", 0)
	}
	return &PollingDemandSource{
		auth: auth,
		newClient: func(pat string) *gh.Client {
			return gh.NewClient(nil).WithAuthToken(pat)
		},
		debug: debug,
	}
}

// Poll implements scheduler.DemandSource.
func (p *PollingDemandSource) Poll(ctx context.Context, targets []scheduler.TargetRef) ([]scheduler.DemandSnapshot, error) {
	if p.debug == nil {
		p.debug = log.New(io.Discard, "", 0)
	}
	pat, err := p.auth(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve PAT: %w", err)
	}
	client := p.newClient(pat)

	snapshots := make([]scheduler.DemandSnapshot, 0, len(targets))
	for _, t := range targets {
		n, err := p.countQueuedJobs(ctx, client, t)
		if err != nil {
			return nil, fmt.Errorf("count queued jobs for %s: %w", t.Key(), err)
		}
		snapshots = append(snapshots, scheduler.DemandSnapshot{
			Target:     t,
			QueuedJobs: n,
		})
	}
	return snapshots, nil
}

// countQueuedJobs counts queued workflow jobs for a repo whose labels match
// the target's expected runner labels. GitHub has no single "queued jobs for
// repo" endpoint, so this lists queued workflow runs, then lists jobs per run
// and filters by status + label match.
func (p *PollingDemandSource) countQueuedJobs(ctx context.Context, client *gh.Client, t scheduler.TargetRef) (int, error) {
	runs, _, err := client.Actions.ListRepositoryWorkflowRuns(ctx, t.Owner, t.Repo, &gh.ListWorkflowRunsOptions{
		Status: "queued",
	})
	if err != nil {
		return 0, err
	}
	p.debug.Printf("github: %s: %d queued workflow run(s)", t.Key(), len(runs.WorkflowRuns))

	count := 0
	for _, run := range runs.WorkflowRuns {
		jobs, _, err := client.Actions.ListWorkflowJobs(ctx, t.Owner, t.Repo, run.GetID(), &gh.ListWorkflowJobsOptions{
			Filter: "latest",
		})
		if err != nil {
			return 0, err
		}
		for _, job := range jobs.Jobs {
			if job.GetStatus() != "queued" {
				continue
			}
			match := labelsMatch(t.Labels, job.Labels)
			p.debug.Printf("github: %s: run %d job %q status=%s labels=%v wanted=%v match=%v",
				t.Key(), run.GetID(), job.GetName(), job.GetStatus(), job.Labels, t.Labels, match)
			if match {
				count++
			}
		}
	}
	p.debug.Printf("github: %s: %d job(s) matched labels out of queued jobs seen", t.Key(), count)
	return count, nil
}

// labelsMatch reports whether every label the target expects is present on
// the job (job.Labels comes from the workflow's `runs-on:` key). An empty
// expected label set matches any job.
func labelsMatch(expected, actual []string) bool {
	if len(expected) == 0 {
		return true
	}
	actualSet := make(map[string]bool, len(actual))
	for _, l := range actual {
		actualSet[l] = true
	}
	for _, want := range expected {
		if !actualSet[want] {
			return false
		}
	}
	return true
}
