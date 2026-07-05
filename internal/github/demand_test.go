package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	gh "github.com/google/go-github/v66/github"
	"github.com/usebruno/mac-action-agent/internal/scheduler"
)

// newTestServerClient builds a go-github client pointed at a local
// httptest.Server serving canned JSON responses, keyed by request path.
func newTestServerClient(t *testing.T, handlers map[string]http.HandlerFunc) (*gh.Client, func()) {
	t.Helper()
	mux := http.NewServeMux()
	for path, h := range handlers {
		mux.HandleFunc(path, h)
	}
	server := httptest.NewServer(mux)

	client := gh.NewClient(nil)
	baseURL, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	client.BaseURL = baseURL

	return client, server.Close
}

func jsonHandler(t *testing.T, v interface{}) http.HandlerFunc {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}
}

func TestPollingDemandSource_CountsOnlyQueuedMatchingLabels(t *testing.T) {
	runs := &gh.WorkflowRuns{
		WorkflowRuns: []*gh.WorkflowRun{
			{ID: gh.Int64(1)},
			{ID: gh.Int64(2)},
		},
	}
	jobsRun1 := &gh.Jobs{
		Jobs: []*gh.WorkflowJob{
			{Status: gh.String("queued"), Labels: []string{"self-hosted", "macOS", "ARM64"}},
			{Status: gh.String("in_progress"), Labels: []string{"self-hosted", "macOS", "ARM64"}},
		},
	}
	jobsRun2 := &gh.Jobs{
		Jobs: []*gh.WorkflowJob{
			{Status: gh.String("queued"), Labels: []string{"self-hosted", "linux", "X64"}},
			{Status: gh.String("queued"), Labels: []string{"self-hosted", "macOS", "ARM64"}},
		},
	}

	client, closeFn := newTestServerClient(t, map[string]http.HandlerFunc{
		"/repos/acme/repo1/actions/runs":        jsonHandler(t, runs),
		"/repos/acme/repo1/actions/runs/1/jobs": jsonHandler(t, jobsRun1),
		"/repos/acme/repo1/actions/runs/2/jobs": jsonHandler(t, jobsRun2),
	})
	defer closeFn()

	ds := &PollingDemandSource{
		auth:      func(ctx context.Context) (string, error) { return "fake-pat", nil },
		newClient: func(pat string) *gh.Client { return client },
	}

	target := scheduler.TargetRef{Owner: "acme", Repo: "repo1", Labels: []string{"self-hosted", "macOS", "ARM64"}}
	snapshots, err := ds.Poll(context.Background(), []scheduler.TargetRef{target})
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	// run1 has 1 queued+matching job, run2 has 1 queued+matching job (linux one doesn't match) = 2
	if snapshots[0].QueuedJobs != 2 {
		t.Errorf("QueuedJobs = %d, want 2", snapshots[0].QueuedJobs)
	}
}

func TestPollingDemandSource_NoQueuedRuns(t *testing.T) {
	client, closeFn := newTestServerClient(t, map[string]http.HandlerFunc{
		"/repos/acme/repo1/actions/runs": jsonHandler(t, &gh.WorkflowRuns{}),
	})
	defer closeFn()

	ds := &PollingDemandSource{
		auth:      func(ctx context.Context) (string, error) { return "fake-pat", nil },
		newClient: func(pat string) *gh.Client { return client },
	}

	target := scheduler.TargetRef{Owner: "acme", Repo: "repo1"}
	snapshots, err := ds.Poll(context.Background(), []scheduler.TargetRef{target})
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if snapshots[0].QueuedJobs != 0 {
		t.Errorf("QueuedJobs = %d, want 0", snapshots[0].QueuedJobs)
	}
}

func TestPollingDemandSource_AuthErrorPropagates(t *testing.T) {
	ds := &PollingDemandSource{
		auth: func(ctx context.Context) (string, error) {
			return "", errFake("no PAT available")
		},
	}
	_, err := ds.Poll(context.Background(), []scheduler.TargetRef{{Owner: "a", Repo: "b"}})
	if err == nil {
		t.Fatal("expected error when auth fails")
	}
}

func TestLabelsMatch(t *testing.T) {
	tests := []struct {
		name     string
		expected []string
		actual   []string
		want     bool
	}{
		{"empty expected matches anything", nil, []string{"self-hosted"}, true},
		{"exact match", []string{"self-hosted", "macOS"}, []string{"self-hosted", "macOS"}, true},
		{"subset match", []string{"self-hosted"}, []string{"self-hosted", "macOS", "ARM64"}, true},
		{"missing label fails", []string{"self-hosted", "linux"}, []string{"self-hosted", "macOS"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := labelsMatch(tt.expected, tt.actual); got != tt.want {
				t.Errorf("labelsMatch(%v, %v) = %v, want %v", tt.expected, tt.actual, got, tt.want)
			}
		})
	}
}

type errFake string

func (e errFake) Error() string { return string(e) }
