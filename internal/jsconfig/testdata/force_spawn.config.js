module.exports = {
  auth: function() {
    return "static-pat-value";
  },
  targets: [
    { owner: "acme", repo: "solo" },
  ],
  forceSpawn: true,
};
