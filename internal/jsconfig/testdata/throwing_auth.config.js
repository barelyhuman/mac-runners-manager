module.exports = {
  auth: function() {
    throw new Error("keychain access denied");
  },
  targets: [{ owner: "acme", repo: "repo1" }],
};
