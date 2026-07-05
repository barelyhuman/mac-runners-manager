module.exports = {
  auth: function() {
    return env("TEST_PAT");
  },
  targets: [
    { owner: "acme", repo: "repo1", labels: ["self-hosted", "macOS", "ARM64"] },
    { owner: "acme", repo: "repo2", labels: ["self-hosted", "linux"] },
  ],
  poolSize: 3,
  tickIntervalSeconds: 45,
  priority: function(state) {
    var weights = {};
    state.targets.forEach(function(t) {
      weights[t.owner + "/" + t.repo] = t.queuedJobs * 2;
    });
    return weights;
  }
};
