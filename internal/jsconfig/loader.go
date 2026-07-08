// Package jsconfig loads and executes the user-authored JS config file that
// resolves the GitHub PAT, lists target repos, and optionally supplies a
// custom priority/allocation function.
package jsconfig

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/barelyhuman/mac-runners-manager/internal/scheduler"
)

const (
	defaultTickInterval = 30 * time.Second
	defaultPoolSize     = 2
	callTimeout         = 10 * time.Second
)

// ResolvedConfig is the fully-decoded result of loading a config.js file.
type ResolvedConfig struct {
	Auth           func(ctx context.Context) (string, error)
	Priority       scheduler.PriorityFunc
	Targets        []scheduler.TargetRef
	PoolSize       int
	TickInterval   time.Duration
	ForceSpawn     bool
	RunnerVersion  string         // optional: GitHub Actions runner version tag, e.g. "2.335.1"
	VMMemoryMB     int            // optional: VM memory size in MB (e.g. 4096 = 4GB)
	SSHCredentials SSHCredentials // optional: overrides CLI flags if set
}

// SSHCredentials describes how the agent should authenticate to VM guests
type SSHCredentials struct {
	User     string
	Password string
	KeyPath  string
}

// JSConfig wraps a loaded goja.Runtime. goja.Runtime is not safe for
// concurrent use, so all calls into it (Auth, Priority) are serialized
// behind a mutex.
type JSConfig struct {
	mu       sync.Mutex
	vm       *goja.Runtime
	authFn   goja.Callable
	priority goja.Callable // nil if config didn't define one
}

// Load reads, compiles, and executes the config file at path, then decodes
// module.exports into a ResolvedConfig.
func Load(path string) (*ResolvedConfig, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	vm := goja.New()
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", false))
	registerHostFunctions(vm)

	module := vm.NewObject()
	exports := vm.NewObject()
	if err := module.Set("exports", exports); err != nil {
		return nil, fmt.Errorf("seed module.exports: %w", err)
	}
	if err := vm.Set("module", module); err != nil {
		return nil, fmt.Errorf("seed module global: %w", err)
	}

	program, err := goja.Compile(path, string(src), false)
	if err != nil {
		return nil, fmt.Errorf("compile config script: %w", err)
	}
	if _, err := vm.RunProgram(program); err != nil {
		return nil, fmt.Errorf("run config script: %w", err)
	}

	finalExports := vm.Get("module").ToObject(vm).Get("exports")
	exportsObj := finalExports.ToObject(vm)
	if exportsObj == nil {
		return nil, fmt.Errorf("config script did not set module.exports to an object")
	}

	jc := &JSConfig{vm: vm}

	authVal := exportsObj.Get("auth")
	authFn, ok := goja.AssertFunction(authVal)
	if !ok {
		return nil, fmt.Errorf("config.js must export an auth() function")
	}
	jc.authFn = authFn

	if priorityVal := exportsObj.Get("priority"); priorityVal != nil && !goja.IsUndefined(priorityVal) {
		if fn, ok := goja.AssertFunction(priorityVal); ok {
			jc.priority = fn
		}
	}

	targets, err := decodeTargets(vm, exportsObj)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("config.js must export at least one target")
	}

	poolSize := defaultPoolSize
	if v := exportsObj.Get("poolSize"); v != nil && !goja.IsUndefined(v) {
		poolSize = int(v.ToInteger())
	}

	tickInterval := defaultTickInterval
	if v := exportsObj.Get("tickIntervalSeconds"); v != nil && !goja.IsUndefined(v) {
		tickInterval = time.Duration(v.ToInteger()) * time.Second
	}

	forceSpawn := false
	if v := exportsObj.Get("forceSpawn"); v != nil && !goja.IsUndefined(v) {
		forceSpawn = v.ToBoolean()
	}

	runnerVersion := ""
	if v := exportsObj.Get("runnerVersion"); v != nil && !goja.IsUndefined(v) {
		runnerVersion = v.String()
	}

	vmMemoryMB := 0
	if v := exportsObj.Get("vmMemoryMB"); v != nil && !goja.IsUndefined(v) {
		vmMemoryMB = int(v.ToInteger())
	}

	sshCreds := SSHCredentials{}
	if v := exportsObj.Get("sshCredentials"); v != nil && !goja.IsUndefined(v) {
		if m, ok := v.Export().(map[string]interface{}); ok {
			if u, ok := m["user"].(string); ok {
				sshCreds.User = u
			}
			if p, ok := m["password"].(string); ok {
				sshCreds.Password = p
			}
			if k, ok := m["keyPath"].(string); ok {
				sshCreds.KeyPath = k
			}
		}
	}

	rc := &ResolvedConfig{
		Auth:          jc.Auth,
		Targets:       targets,
		PoolSize:      poolSize,
		TickInterval:  tickInterval,
		ForceSpawn:    forceSpawn,
		RunnerVersion: runnerVersion,
		VMMemoryMB:    vmMemoryMB,
		SSHCredentials: sshCreds,
	}
	if jc.priority != nil {
		rc.Priority = jc.Priority
	}
	return rc, nil
}

func decodeTargets(vm *goja.Runtime, exportsObj *goja.Object) ([]scheduler.TargetRef, error) {
	targetsVal := exportsObj.Get("targets")
	if targetsVal == nil || goja.IsUndefined(targetsVal) {
		return nil, fmt.Errorf("config.js must export a targets array")
	}
	exported := targetsVal.Export()
	rawList, ok := exported.([]interface{})
	if !ok {
		return nil, fmt.Errorf("config.js targets must be an array")
	}

	targets := make([]scheduler.TargetRef, 0, len(rawList))
	for i, raw := range rawList {
		m, ok := raw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("targets[%d] must be an object", i)
		}
		owner, _ := m["owner"].(string)
		repo, _ := m["repo"].(string)
		if owner == "" || repo == "" {
			return nil, fmt.Errorf("targets[%d] must have non-empty owner and repo", i)
		}
		var labels []string
		if rawLabels, ok := m["labels"].([]interface{}); ok {
			for _, l := range rawLabels {
				if s, ok := l.(string); ok {
					labels = append(labels, s)
				}
			}
		}
		targets = append(targets, scheduler.TargetRef{Owner: owner, Repo: repo, Labels: labels})
	}
	return targets, nil
}

// Auth calls the config's auth() function, serialized against the shared
// goja.Runtime.
func (jc *JSConfig) Auth(ctx context.Context) (string, error) {
	jc.mu.Lock()
	defer jc.mu.Unlock()

	jc.vm.ClearInterrupt()
	timer := time.AfterFunc(callTimeout, func() { jc.vm.Interrupt("auth() timed out") })
	defer timer.Stop()

	result, err := jc.authFn(goja.Undefined())
	if err != nil {
		return "", fmt.Errorf("config.js auth() failed: %w", err)
	}
	pat, ok := result.Export().(string)
	if !ok || pat == "" {
		return "", fmt.Errorf("config.js auth() must return a non-empty string")
	}
	return pat, nil
}

// Priority calls the config's priority() function, if defined. Returns a nil
// map (meaning "use default weighting") if priority() is not configured.
func (jc *JSConfig) Priority(state scheduler.SchedulerState) (map[string]float64, error) {
	if jc.priority == nil {
		return nil, nil
	}

	jc.mu.Lock()
	defer jc.mu.Unlock()

	jc.vm.ClearInterrupt()
	timer := time.AfterFunc(callTimeout, func() { jc.vm.Interrupt("priority() timed out") })
	defer timer.Stop()

	stateVal := jc.vm.ToValue(state)
	result, err := jc.priority(goja.Undefined(), stateVal)
	if err != nil {
		return nil, fmt.Errorf("config.js priority() failed: %w", err)
	}

	exported := result.Export()
	raw, ok := exported.(map[string]interface{})
	if !ok {
		return nil, nil
	}
	weights := make(map[string]float64, len(raw))
	for k, v := range raw {
		switch n := v.(type) {
		case int64:
			weights[k] = float64(n)
		case float64:
			weights[k] = n
		}
	}
	return weights, nil
}
