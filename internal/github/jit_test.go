package github

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	gh "github.com/google/go-github/v66/github"
	"github.com/barelyhuman/mac-runners-manager/internal/scheduler"
)

func TestJITRegistrar_GenerateJITConfig(t *testing.T) {
	var capturedBody gh.GenerateJITConfigRequest
	resp := &gh.JITRunnerConfig{EncodedJITConfig: gh.String("dGVzdC1jb25maWc=")}

	client, closeFn := newTestServerClient(t, map[string]http.HandlerFunc{
		"/repos/acme/repo1/actions/runners/generate-jitconfig": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&capturedBody)
			w.Header().Set("Content-Type", "application/json")
			body, _ := json.Marshal(resp)
			w.Write(body)
		},
	})
	defer closeFn()

	reg := &JITRegistrar{
		auth:      func(ctx context.Context) (string, error) { return "fake-pat", nil },
		newClient: func(pat string) *gh.Client { return client },
	}

	target := scheduler.TargetRef{Owner: "acme", Repo: "repo1", Labels: []string{"self-hosted", "macOS"}}
	payload, err := reg.GenerateJITConfig(context.Background(), target, "runner-abc123")
	if err != nil {
		t.Fatalf("GenerateJITConfig: %v", err)
	}
	if payload.JITConfig != "dGVzdC1jb25maWc=" {
		t.Errorf("JITConfig = %q, want encoded config", payload.JITConfig)
	}
	if capturedBody.Name != "runner-abc123" {
		t.Errorf("request Name = %q, want runner-abc123", capturedBody.Name)
	}
	if len(capturedBody.Labels) != 2 {
		t.Errorf("request Labels = %v, want 2 labels", capturedBody.Labels)
	}
}

func TestJITRegistrar_DefaultsLabelsWhenEmpty(t *testing.T) {
	var capturedBody gh.GenerateJITConfigRequest
	resp := &gh.JITRunnerConfig{EncodedJITConfig: gh.String("xyz")}

	client, closeFn := newTestServerClient(t, map[string]http.HandlerFunc{
		"/repos/acme/repo1/actions/runners/generate-jitconfig": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&capturedBody)
			w.Header().Set("Content-Type", "application/json")
			body, _ := json.Marshal(resp)
			w.Write(body)
		},
	})
	defer closeFn()

	reg := &JITRegistrar{
		auth:      func(ctx context.Context) (string, error) { return "fake-pat", nil },
		newClient: func(pat string) *gh.Client { return client },
	}

	target := scheduler.TargetRef{Owner: "acme", Repo: "repo1"}
	_, err := reg.GenerateJITConfig(context.Background(), target, "runner-1")
	if err != nil {
		t.Fatalf("GenerateJITConfig: %v", err)
	}
	if len(capturedBody.Labels) != 1 || capturedBody.Labels[0] != "self-hosted" {
		t.Errorf("expected default self-hosted label, got %v", capturedBody.Labels)
	}
}

func TestJITRegistrar_AuthErrorPropagates(t *testing.T) {
	reg := &JITRegistrar{
		auth: func(ctx context.Context) (string, error) { return "", errFake("no PAT") },
	}
	_, err := reg.GenerateJITConfig(context.Background(), scheduler.TargetRef{Owner: "a", Repo: "b"}, "r")
	if err == nil {
		t.Fatal("expected error when auth fails")
	}
}
