package github

import (
	"context"
	"fmt"
	"io"
	"log"

	gh "github.com/google/go-github/v66/github"
	"github.com/usebruno/mac-action-agent/internal/scheduler"
)

// defaultRunnerGroupID is the built-in "Default" runner group present on
// every repo/org; users with custom runner groups can extend this later.
const defaultRunnerGroupID = 1

// JITRegistrar implements scheduler.RunnerRegistrar by minting one-time
// ephemeral runner registrations via GitHub's JIT config API.
type JITRegistrar struct {
	auth      AuthFunc
	newClient func(pat string) *gh.Client
	debug     *log.Logger
}

// NewJITRegistrar constructs a registrar that authenticates using the given
// AuthFunc. debug receives verbose tracing of JIT config requests; pass nil
// to disable it.
func NewJITRegistrar(auth AuthFunc, debug *log.Logger) *JITRegistrar {
	if debug == nil {
		debug = log.New(io.Discard, "", 0)
	}
	return &JITRegistrar{
		auth: auth,
		newClient: func(pat string) *gh.Client {
			return gh.NewClient(nil).WithAuthToken(pat)
		},
		debug: debug,
	}
}

// GenerateJITConfig implements scheduler.RunnerRegistrar.
func (r *JITRegistrar) GenerateJITConfig(ctx context.Context, target scheduler.TargetRef, runnerName string) (scheduler.BootPayload, error) {
	if r.debug == nil {
		r.debug = log.New(io.Discard, "", 0)
	}
	pat, err := r.auth(ctx)
	if err != nil {
		return scheduler.BootPayload{}, fmt.Errorf("resolve PAT: %w", err)
	}
	client := r.newClient(pat)

	labels := target.Labels
	if len(labels) == 0 {
		labels = []string{"self-hosted"}
	}

	r.debug.Printf("github: %s: requesting JIT config for runner %q, labels=%v", target.Key(), runnerName, labels)
	cfg, _, err := client.Actions.GenerateRepoJITConfig(ctx, target.Owner, target.Repo, &gh.GenerateJITConfigRequest{
		Name:          runnerName,
		RunnerGroupID: defaultRunnerGroupID,
		Labels:        labels,
	})
	if err != nil {
		return scheduler.BootPayload{}, fmt.Errorf("generate JIT config for %s: %w", target.Key(), err)
	}
	r.debug.Printf("github: %s: JIT config generated for runner %q", target.Key(), runnerName)

	return scheduler.BootPayload{
		JITConfig: cfg.GetEncodedJITConfig(),
	}, nil
}
