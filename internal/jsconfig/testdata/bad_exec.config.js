module.exports = {
  auth: function() {
    exec("false"); // always exits non-zero
    return "pat";
  },
  targets: [{ owner: "acme", repo: "repo1" }],
};
