# Consul fork of OpenBao

This fork of OpenBao, maintained by contain-security, **restores the Consul
support that upstream removed**:

- Consul **storage backend**: `internal/physical/consul`
- Consul **service registration**: `internal/serviceregistration/consul`

## Status / warnings

- Use at your own risk. Upstream does not support this configuration.
- The Consul code logs more than it needs to at Info level (in
  `internal/physical/consul/consul.go` and
  `internal/serviceregistration/consul/consul.go`). Quiet it before production
  use.

## Branches

There is one branch per upstream minor release, **`release/X.Y.x`**. Each is
the upstream release tag plus the Consul port. The newest one is the
repository's default branch. Nothing else is long-lived, and there is no CI:
GitHub Actions is disabled on this repository, and every step below is manual.

## How Consul is wired in

The port is almost entirely additive, so it carries across upstream releases
with little or no conflict:

| File | Change |
|---|---|
| `internal/physical/consul/` | New. Storage backend and tests |
| `internal/serviceregistration/consul/` | New. Service registration and tests |
| `internal/command/commands_consul.go` | New. An `init()` that adds `"consul"` to the `physicalBackends` and `serviceRegistrations` maps, so upstream's `commands.go` stays untouched |
| `go.mod` / `go.sum` | Adds `github.com/hashicorp/consul/api` |
| `README.md` | Fork notice prepended above the unmodified upstream README |
| `CONSUL_FORK.md` | This file |
| `.gitattributes` | `go.sum merge=union`, so go.sum merges cleanly |
| `.github/dependabot.yml` | Deleted, so the fork never opens Dependabot version-update PRs (Dependabot alerts and security updates are also turned off in the repository settings) |

## Release procedure

### New upstream patch release (e.g. v2.7.1 to v2.7.2)

```sh
git fetch upstream --tags
git checkout release/2.7.x
git merge v2.7.2            # merge the tag, not upstream's branch
# If go.mod conflicts: keep both sides' require lines by hand, then
go mod tidy
# build and test (below), commit, then:
git push origin release/2.7.x
```

### New upstream minor release (e.g. v2.8.0)

```sh
git fetch upstream --tags
git checkout -b release/2.8.x v2.8.0
# Copy the port across from the previous release branch
git checkout release/2.7.x -- internal/physical/consul internal/serviceregistration/consul \
    internal/command/commands_consul.go CONSUL_FORK.md .gitattributes
git rm .github/dependabot.yml
{ git show release/2.7.x:README.md | head -n 23; cat README.md; } > README.md.new && mv README.md.new README.md
go get github.com/hashicorp/consul/api@latest && go mod tidy
git diff go.mod    # check that only Consul-related lines changed
# build and test (below), commit, then:
git push -u origin release/2.8.x
gh repo edit contain-security/openbao --default-branch release/2.8.x
```

If upstream has moved a package the Consul code depends on (for example the
`physical` or `serviceregistration` interfaces), fix the compile errors on the
new branch before you push it.

### Consul fixes

Commit them directly to the newest `release/X.Y.x`. If an older release branch
is still in use, cherry-pick the fix onto it as well.

## Build and test

```sh
go build ./... && go vet ./internal/physical/consul/... ./internal/serviceregistration/consul/... ./internal/command/...
```

The Consul tests run against a real Consul agent. Setting
`OPENBAO_CONSUL_TEST=1` makes a missing Consul a hard failure instead of a skip:

```sh
docker run -d --rm --name consul-test -p 127.0.0.1:8500:8500 hashicorp/consul:latest agent -dev -client=0.0.0.0
OPENBAO_CONSUL_TEST=1 CONSUL_HTTP_ADDR=127.0.0.1:8500 \
  go test -count=1 -timeout 20m ./internal/physical/consul/... ./internal/serviceregistration/consul/...
docker stop consul-test
```

Without `OPENBAO_CONSUL_TEST=1` and with no Consul running, the live tests skip,
which is fine on a laptop. The config-parsing and mocked tests always run.

The live TLS tests in `consul_tls_test.go` skip unless `CONSUL_HTTPS_ADDR` (and
`CONSUL_CACERT`) point at a TLS-enabled Consul agent. The commands above don't
set them.

Before committing, `go mod tidy -diff` should print nothing.
