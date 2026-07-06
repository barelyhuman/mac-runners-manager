package vmprovisioner

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
)

// defaultFallbackVersion is used when the latest runner version cannot be
// resolved from the GitHub API.
const defaultFallbackVersion = "2.335.1"

// githubLatestReleaseURL is the GitHub releases API endpoint for the runner.
const githubLatestReleaseURL = "https://api.github.com/repos/actions/runner/releases/latest"

// runnerDownloadURL returns the download URL for the GitHub Actions runner
// tarball matching the requested version tag and the host's OS/architecture.
// If versionTag is empty, it fetches the latest release from GitHub.
func runnerDownloadURL(versionTag string) (string, error) {
	if versionTag == "" {
		latest, err := fetchLatestRunnerVersion()
		if err != nil {
			versionTag = defaultFallbackVersion
		} else {
			versionTag = latest
		}
	}

	os, arch := platform()
	filename := fmt.Sprintf("actions-runner-%s-%s-%s.tar.gz", os, arch, versionTag)
	return fmt.Sprintf("https://github.com/actions/runner/releases/download/v%s/%s", versionTag, filename), nil
}

// fetchLatestRunnerVersion queries the GitHub API for the latest runner
// release tag name (e.g. "v2.335.1") and strips the leading "v".
func fetchLatestRunnerVersion() (string, error) {
	resp, err := http.Get(githubLatestReleaseURL)
	if err != nil {
		return "", fmt.Errorf("fetch latest runner release: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("github releases API returned %d", resp.StatusCode)
	}

	var payload struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode release payload: %w", err)
	}

	tag := payload.TagName
	if len(tag) > 0 && tag[0] == 'v' {
		tag = tag[1:]
	}
	if tag == "" {
		return "", fmt.Errorf("latest release tag name is empty")
	}
	return tag, nil
}

// platform converts Go's GOOS/GOARCH pair into the naming convention used by
// the actions/runner release artifacts.
func platform() (os string, arch string) {
	switch runtime.GOOS {
	case "darwin":
		os = "osx"
	case "linux":
		os = "linux"
	case "windows":
		os = "win"
	default:
		os = "linux"
	}

	switch runtime.GOARCH {
	case "amd64":
		arch = "x64"
	case "arm64":
		arch = "arm64"
	case "arm":
		arch = "arm"
	default:
		arch = "x64"
	}
	return os, arch
}
