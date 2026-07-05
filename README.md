# mac-action-agent

A tiny Go binary that manages a small, fixed pool of [Tart](https://github.com/cirruslabs/tart) VMs (macOS or Linux) and cycles them between GitHub repos as ephemeral, JIT-registered self-hosted Actions runners.

It polls each configured repo for queued jobs, decides which repo should get the next free VM using a fairness-aware allocation algorithm, and provisions a one-shot runner via GitHub's JIT runner registration API. When a VM's job finishes, the runner self-deregisters and the VM becomes available for reassignment to another repo on the next tick.

Apple's EULA caps concurrent macOS VMs at 2 on a single physical Mac — the pool size defaults to 2 for this reason, but is configurable.

## How allocation works

Given `N` idle VMs and demand from multiple repos, each tick:

1. **Guarantee phase** — every repo with at least one queued job gets 1 VM, up to pool capacity, so no repo is starved.
2. **Remainder phase** — any leftover VMs are distributed proportionally to queue depth (or a custom weight from `priority()` in config).
3. **Clamp** — no repo is ever given more VMs than it has queued jobs.

Example: Repo1 has 2 queued jobs, Repo2 has 5, pool size is 2 → each gets 1 VM. If Repo1 has 0 queued jobs and Repo2 has 5, both VMs go to Repo2.

## Prerequisites

- [Tart](https://github.com/cirruslabs/tart) installed (`brew install cirruslabs/cli/tart`).
- A golden base VM image, pre-built with a boot-time agent (`launchd` on macOS, `systemd` on Linux) that reads a mounted directory for a JIT runner config and registers with GitHub. Building this golden image is outside the scope of this binary.
- A GitHub PAT (or GitHub App token) with permission to manage self-hosted runners on the target repos.

## Configuration

Runtime behavior is controlled by a JS config file (see [configs/example.config.js](configs/example.config.js)) that exports:

- `auth()` — resolves the GitHub PAT (from env, 1Password CLI, Keychain, etc.)
- `targets` — the list of `{ owner, repo, labels }` repos to watch
- `poolSize`, `tickIntervalSeconds` — pool size and poll interval
- `priority(state)` (optional) — custom weighting function for allocation
- `forceSpawn` (optional, single-target configs only) — immediately fill the pool for the one target at startup, ignoring queued-job demand; a one-shot action, normal demand-based allocation resumes on later ticks

The config file runs in an embedded JS engine (goja) with two host functions: `env(name)` and `exec(cmd, ...args)`. Treat the config file as trusted code you own, not untrusted input.

## Running

```sh
go build -o mac-action-agent ./cmd/mac-action-agent
./mac-action-agent -config configs/example.config.js -base-image my-golden-image -tart-binary tart
```

Flags:

- `-config` — path to the JS config file (default `config.js`)
- `-base-image` — golden Tart image to clone for each ephemeral VM (required)
- `-tart-binary` — path to the `tart` CLI (default `tart`, resolved via `PATH`)
- `-state-dir` — scratch directory for per-VM boot payloads (default `/tmp/mac-action-agent`)
- `-net-bridged` — host network interface (e.g. `en0`) to bridge VMs onto so the host can SSH into them directly by IP; leave empty to use tart's default shared/NAT networking. Requires Remote Login to be enabled on the golden image. Run `tart run <image> --net-bridged=list` to see available interface names.

## Development

```sh
go test ./...
```

Pure logic (allocation algorithm, config parsing, CLI argument construction) is unit tested without needing a real `tart` binary or GitHub API access. Actual VM boot behavior, golden-image boot-time registration, and end-to-end runner lifecycle require manual testing against real hardware and a test repo.

## TODO 
- [ ] Custom creds for VMs
- [ ] Better Garbage Cleaner
- [ ] Sanity check before adopting running VM's on booting the agent 
- [ ] Guide to provisioning as a service for macos.

