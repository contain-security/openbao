# Experimental Consul fork of OpenBao

This is an experimental fork of OpenBao maintained by contain-security that **restores Consul
support that was removed upstream**:

- Consul **storage backend** — `physical/consul`
- Consul **service registration** — `serviceregistration/consul`

## Status / warnings

- Use at your own risk. This is **not** an upstream-supported configuration.
- Some extra debug logging is currently enabled in the Consul code and should be quieted/removed
  before any production use (verbose logging in `physical/consul/consul.go` and
  `serviceregistration/consul/consul.go`).

## How Consul is wired in (additive design)

Registration is injected **additively** via [`command/commands_consul.go`](command/commands_consul.go) —
an `init()` that appends `"consul"` to the `physicalBackends` and `serviceRegistrations` maps. This
keeps `command/commands.go` and `README.md` byte-identical to upstream, so upstream syncs do not
conflict on them. All other Consul code lives at paths that do not exist upstream
(`physical/consul/*`, `serviceregistration/consul/*`), so they never conflict either.

The only fork-modified files shared with upstream are `go.mod` / `go.sum`, handled below.

## Upstream sync / merge procedure

1. `git fetch upstream && git merge upstream/main` (merge only — never rebase/force-push).
2. Expect conflicts **only** in `go.mod` / `go.sum`. `go.sum` is set to `merge=union` in
   `.gitattributes`, so it usually auto-resolves; `go.mod` is resolved by the next step.
3. `go mod tidy` — reconciles module requirements. The Consul dependencies are retained because
   `command/commands_consul.go` imports them.
4. `go build ./... && go vet ./...`
5. Run the Consul test suite against a real Consul (`OPENBAO_CONSUL_TEST=1`, see the consul test
   files) and the GPG plugin test-bed.

## Testing the Consul backend

The Consul tests run against a real Consul agent. Set `OPENBAO_CONSUL_TEST=1` to turn
"Consul-unavailable" skips into hard failures (CI does this so coverage is guaranteed):

```
consul agent -dev &
OPENBAO_CONSUL_TEST=1 CONSUL_HTTP_ADDR=127.0.0.1:8500 \
  go test -v -count=1 -timeout 20m -run Consul \
  ./physical/consul/... ./serviceregistration/consul/...
```

Without `OPENBAO_CONSUL_TEST=1` and with no Consul running, the live tests skip gracefully (useful
on a developer laptop); the config-parsing and mocked tests always run.
