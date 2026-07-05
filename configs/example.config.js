// Example mac-action-agent config.
//
// Host functions available in this file:
//   env(name)             - read an environment variable
//   exec(cmd, ...args)    - run a command, return trimmed stdout, throw on failure
//   log(...)              - print a message via the agent's logger
//
// This file is executed once at startup by an embedded JS engine (goja).
// There is no filesystem/network access beyond env() and exec() - treat it
// as a small, trusted script you own, not untrusted input.

module.exports = {
  // Called at startup (and again whenever the agent needs a fresh PAT) to
  // resolve the GitHub token used for polling and runner registration.
  auth: function () {
    // Simple env var:
    return env("GITHUB_PAT");

    // Or pull from 1Password CLI:
    // return exec("op", "read", "op://Private/github-pat/credential");

    // Or macOS Keychain:
    // return exec("security", "find-generic-password", "-s", "github-pat", "-w");
  },

  // Repos this agent watches and can register runners against.
  targets: [
    { owner: "usebruno", repo: "bruno", labels: ["self-hosted", "macOS", "ARM64", "macos-latest"] },
  ],

  // Max concurrent VMs. Apple's EULA caps concurrent macOS VMs at 2 on a
  // single physical Mac.
  poolSize: 1,

  // How often to poll GitHub for queued jobs.
  tickIntervalSeconds: 30,

  // Optional, single-target configs only: immediately provision the entire
  // pool for the one target at startup, ignoring queued-job demand. Useful
  // when GitHub's queued-job signal is slow/unreliable and you'd rather
  // always have the pool warm. This is a one-shot startup action; normal
  // demand-based allocation resumes on subsequent ticks. Defaults to false.
  forceSpawn: true,

  // Optional: customize how idle VM capacity is weighted across targets.
  // Return an object mapping "owner/repo" -> weight (higher wins more
  // capacity). Omit this function entirely, or return {}, to use the
  // default: weight = queuedJobs.
  priority: function (state) {
    var weights = {};
    state.targets.forEach(function (t) {
      var key = t.owner + "/" + t.repo;
      var weight = t.queuedJobs;

      // Example bias: always prioritize "bruno" over other repos.
      if (t.repo === "bruno") {
        weight *= 2;
      }

      weights[key] = weight;
    });
    return weights;
  },
};
