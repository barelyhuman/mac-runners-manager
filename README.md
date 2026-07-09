# mac-runners-manager

A tiny Go binary that manages a small, fixed pool of [Tart](https://github.com/cirruslabs/tart) VMs (macOS or Linux) and cycles them between GitHub repos as ephemeral, JIT-registered self-hosted Actions runners.

It polls each configured repo for queued jobs, decides which repo should get the next free VM using a fairness-aware allocation algorithm, and provisions a one-shot runner via GitHub's JIT runner registration API. When a VM's job finishes, the runner self-deregisters and the VM becomes available for reassignment to another repo on the next tick.

Apple's EULA caps concurrent macOS VMs at 2 on a single physical Mac — the pool size defaults to 2 for this reason, but is configurable.

## Quick Start

```sh
go install github.com/barelyhuman/mac-runners-manager/cmd/manager@latest
```

Or, without a local Go toolchain, via [goblin.run](https://goblin.run):

```sh
curl -sf https://goblin.run/github.com/barelyhuman/mac-runners-manager | CMD_PATH=/cmd/manager sh 
```

Then copy [configs/example.config.js](configs/example.config.js), fill in your repo(s) and auth, and run it (see [Running](#running) below).

## Who is it for ?

Anyone running self-hosted GitHub Actions runners on macOS or Linux VMs who wants to share a small, fixed pool of machines across multiple repos instead of dedicating VMs per repo or per job.

Running Tart VMs by hand works fine for a single repo with steady load, but it breaks down once you have several repos competing for the same limited hardware — especially on macOS, where Apple's EULA caps you at 2 concurrent VMs per physical Mac. This agent handles the parts that get tedious to do manually: watching multiple repos for queued jobs, deciding fairly who gets the next free VM, JIT-registering a fresh runner per job (so runners are never reused or left orphaned), and reclaiming the VM the moment the job finishes so it's ready for the next repo in line.

## How allocation works

Given `N` idle VMs and demand from multiple repos, each tick:

1. **Guarantee phase** — every repo with at least one queued job gets 1 VM, up to pool capacity, so no repo is starved.
2. **Remainder phase** — any leftover VMs are distributed proportionally to queue depth (or a custom weight from `priority()` in config).
3. **Clamp** — no repo is ever given more VMs than it has queued jobs.

Example: Repo1 has 2 queued jobs, Repo2 has 5, pool size is 2 → each gets 1 VM. If Repo1 has 0 queued jobs and Repo2 has 5, both VMs go to Repo2.

## Prerequisites

- [Tart](https://github.com/cirruslabs/tart) installed (`brew install cirruslabs/cli/tart`).
- A golden base VM image with Remote Login (SSH) enabled. The agent connects over SSH after boot to download and install the GitHub Actions runner and deliver its JIT registration config — no boot-time agent or mounted directory is required on the image.
- SSH credentials (password or key) for a user on the golden image that can install and run the GitHub Actions runner.
- A GitHub PAT (or GitHub App token) with permission to manage self-hosted runners on the target repos.

## Configuration

Runtime behavior is controlled by a JS config file (see [configs/example.config.js](configs/example.config.js)) that exports:

- `auth()` — resolves the GitHub PAT (from env, 1Password CLI, Keychain, etc.)
- `targets` — the list of `{ owner, repo, labels }` repos to watch
- `poolSize`, `tickIntervalSeconds` — pool size and poll interval
- `priority(state)` (optional) — custom weighting function for allocation
- `forceSpawn` (optional, single-target configs only) — immediately fill the pool for the one target at startup, ignoring queued-job demand; a one-shot action, normal demand-based allocation resumes on later ticks
- `runnerVersion` (optional) — GitHub Actions runner version tag to install (e.g. `"2.335.1"`); if omitted, the agent downloads the latest release automatically
- `vmMemoryMB` (optional) — VM memory size in megabytes (e.g. `4096` = 4 GB); zero or omitted means "use the base image's default"
- `execTimeoutSeconds` (optional) — timeout in seconds for `exec()` calls from the config (e.g. when shelling out to 1Password CLI or macOS Keychain). Default is `60`; increase it for slow-responding commands like `security find-generic-password`
- `sshCredentials` (optional) — `{ user, password }` or `{ user, keyPath }` for connecting to VM guests; CLI flags (`-ssh-user`, `-ssh-password`, `-ssh-key`) override these values

The config file runs in an embedded JS engine (goja) with host functions `env(name)`, `exec(cmd, ...args)`, and `log(...)`. Treat the config file as trusted code you own, not untrusted input.

## Running

```sh
go build -o agent ./cmd/manager
./agent -config configs/example.config.js -base-image my-golden-image -ssh-password admin
```

Flags:

- `-config` — path to the JS config file (default `config.js`)
- `-base-image` — golden Tart image to clone for each ephemeral VM (required)
- `-tart-binary` — path to the `tart` CLI (default `tart`, resolved via `PATH`)
- `-state-dir` — scratch directory for per-VM state and logs (default `/tmp/mac-runners-manager`)
- `-net-bridged` — host network interface (e.g. `en0`) to bridge VMs onto so the host can SSH into them directly by IP; leave empty to use tart's default shared/NAT networking. Requires Remote Login to be enabled on the golden image. Run `tart run <image> --net-bridged=list` to see available interface names.
- `-ssh-user` — SSH user for connecting to VM guests (default `admin`)
- `-ssh-password` — SSH password for VM guests (or set `SSH_DEBUG_PASSWORD`)
- `-ssh-key` — path to a PEM-encoded private key for VM guest SSH
- `-vm-memory` — VM memory size in megabytes (e.g. `4096` = 4 GB); overrides `vmMemoryMB` from config. Zero means "use the base image's default"
- `-verbose` — enable debug logging of demand polling, allocation, and tart CLI calls
- `-tail-runner-logs` — stream each runner's diagnostic logs to the agent's stdout
- `-ssh-debug` — instance name of a booted VM to inspect over SSH, then exit (does not run the scheduler)
- `-ssh-wait` — seconds to wait for `-ssh-debug`'s target VM to report an IP (default `30`)
- `-diag-cmd` — diagnostic command to run over SSH for `-ssh-debug` (repeatable; defaults to a built-in set if omitted)

Exactly one of `-ssh-password` or `-ssh-key` (or the equivalent `sshCredentials` config) is required.

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

