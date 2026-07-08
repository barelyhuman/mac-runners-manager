package tart

import (
	"context"
	"encoding/json"
	"testing"
)

type recordedCall struct {
	name string
	args []string
}

func newFakeClient(t *testing.T, responses map[string][]byte) (*Client, *[]recordedCall) {
	t.Helper()
	calls := &[]recordedCall{}
	c := NewClient("tart", t.TempDir(), nil)
	c.run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		*calls = append(*calls, recordedCall{name: name, args: args})
		key := args[0]
		if resp, ok := responses[key]; ok {
			return resp, nil
		}
		return []byte{}, nil
	}
	return c, calls
}

func TestClone_BuildsExpectedArgs(t *testing.T) {
	c, calls := newFakeClient(t, nil)
	if err := c.Clone(context.Background(), "ghcr.io/base/macos-runner", "instance-1"); err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}
	got := (*calls)[0]
	want := []string{"clone", "ghcr.io/base/macos-runner", "instance-1"}
	if !equalArgs(got.args, want) {
		t.Errorf("Clone args = %v, want %v", got.args, want)
	}
}

func TestBootArgs_IncludesNetBridgedFlagWhenConfigured(t *testing.T) {
	c := NewClient("tart", t.TempDir(), nil).WithNetBridged("en0")
	want := []string{"run", "instance-1", "--no-graphics", "--net-bridged=en0"}
	if got := c.bootArgs("instance-1"); !equalArgs(got, want) {
		t.Errorf("bootArgs = %v, want %v", got, want)
	}
}

func TestBootArgs_OmitsNetBridgedFlagByDefault(t *testing.T) {
	c := NewClient("tart", t.TempDir(), nil)
	want := []string{"run", "instance-1", "--no-graphics"}
	if got := c.bootArgs("instance-1"); !equalArgs(got, want) {
		t.Errorf("bootArgs = %v, want %v", got, want)
	}
}

func TestStop_FallsBackToForce(t *testing.T) {
	calls := &[]recordedCall{}
	c := NewClient("tart", t.TempDir(), nil)
	callCount := 0
	c.run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		*calls = append(*calls, recordedCall{name: name, args: args})
		callCount++
		if callCount == 1 {
			return nil, errFake("graceful stop failed")
		}
		return nil, nil
	}
	if err := c.Stop(context.Background(), "instance-1"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls (graceful + force), got %d", len(*calls))
	}
	if !equalArgs((*calls)[1].args, []string{"stop", "--force", "instance-1"}) {
		t.Errorf("second call args = %v, want force stop", (*calls)[1].args)
	}
}

func TestStop_ReturnsErrorWhenBothFail(t *testing.T) {
	c := NewClient("tart", t.TempDir(), nil)
	c.run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return nil, errFake("nope")
	}
	if err := c.Stop(context.Background(), "instance-1"); err == nil {
		t.Fatal("expected error when both graceful and force stop fail")
	}
}

func TestDelete_BuildsExpectedArgs(t *testing.T) {
	c, calls := newFakeClient(t, nil)
	if err := c.Delete(context.Background(), "instance-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !equalArgs((*calls)[0].args, []string{"delete", "instance-1"}) {
		t.Errorf("Delete args = %v", (*calls)[0].args)
	}
}

func TestSetMemory_BuildsExpectedArgs(t *testing.T) {
	c, calls := newFakeClient(t, nil)
	if err := c.SetMemory(context.Background(), "instance-1", 8192); err != nil {
		t.Fatalf("SetMemory: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}
	got := (*calls)[0]
	want := []string{"set", "instance-1", "--memory", "8192"}
	if !equalArgs(got.args, want) {
		t.Errorf("SetMemory args = %v, want %v", got.args, want)
	}
}

func TestList_ParsesRunningInstancesOnly(t *testing.T) {
	entries := []tartListEntry{
		{Name: "instance-1", Running: true},
		{Name: "instance-2", Running: false},
		{Name: "instance-3", Running: true},
	}
	body, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := newFakeClient(t, map[string][]byte{"list": body})
	got, err := c.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{"instance-1", "instance-3"}
	if !equalArgs(got, want) {
		t.Errorf("List = %v, want %v", got, want)
	}
}

func TestIsRunning(t *testing.T) {
	entries := []tartListEntry{{Name: "instance-1", Running: true}}
	body, _ := json.Marshal(entries)
	c, _ := newFakeClient(t, map[string][]byte{"list": body})

	running, err := c.IsRunning(context.Background(), "instance-1")
	if err != nil || !running {
		t.Errorf("expected instance-1 running, got running=%v err=%v", running, err)
	}

	running, err = c.IsRunning(context.Background(), "instance-missing")
	if err != nil || running {
		t.Errorf("expected instance-missing not running, got running=%v err=%v", running, err)
	}
}

func equalArgs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type errFake string

func (e errFake) Error() string { return string(e) }
