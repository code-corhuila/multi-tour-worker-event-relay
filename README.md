# multi-tour-worker-event-relay

> Worker job 7: event relay between domains (outbox delivery)

Part of the **Multi Tour** distributed system — team `multi-tour`, Grupo 2.
Governance and documentation live in [`multi-tour-docs`](https://github.com/code-corhuila/multi-tour-docs).

## What it is

Worker job 7 (ADR-004 D3, D10): it reads the outbox of each publishing domain through its internal API and delivers every event to the consumers listed in its routing table. It holds no business rule and exposes only `/health`.

## How to start it

Copy `.env.example` to `.env` and set `SERVICE_TOKEN` (issued by the identity service, never versioned). Then either run it with Go 1.26, exporting the variables of `.env` first:

```
go run ./cmd/worker
```

or build the image and start the service from the root composition, which includes `deploy/compose.yml` (the service is `worker-event-relay`; it publishes no port, only `expose 8080` for `/health`):

```
docker compose -f deploy/compose.yml up --build
```

Variables (all read at start; a missing or malformed one stops the worker):

| Variable | Default | Meaning |
|---|---|---|
| `SERVICE_TOKEN` | none, required | Service credential, sent as `Authorization: Bearer` |
| `SECURITY_BASE_URL` | none, required | Internal base URL of `multi-tour-security` (publisher and recipient) |
| `IDENTITY_AUDIT_BASE_URL` | none, required | Internal base URL of `multi-tour-identity-audit-api` (publisher) |
| `RELAY_INTERVAL` | `5s` | Time between two runs |
| `BATCH_SIZE` | `100` | Events handled at most per run |
| `RUN_TIMEOUT` | `30s` | Time limit of one run |
| `HTTP_TIMEOUT` | `5s` | Time limit of each HTTP attempt |
| `HTTP_ATTEMPTS` | `3` | Attempts per call (network, `429` and `5xx` only) |
| `HEALTH_ADDR` | `:8080` | Where `GET /health` listens |

The routing table (which event type goes to which service) lives in `internal/config/routing.go`.

## Where the data is

Nowhere: it keeps no database and no state. Each event stays in the outbox of its publisher.

## How to test it

`gofmt -l .` must print nothing, then `go vet ./...` and `go test ./...` (Go 1.26). The same three run in `ci.yml` on pull requests to `develop` and `qa`.

## What is missing

`multi-tour-worker-runtime` is pinned to a commit of its `develop` (pseudo-version in `go.mod`) until it is tagged. `AccountSessionsRevoked` and `AccountBlocked` have no consumer yet, so they are marked published without delivery. The Docker image is built by hand or by the root composition; `ci.yml` only validates the compose file.

## Branching

Three permanent branches. **None of them accepts a direct commit** — you enter through a child
branch and leave through a Pull Request.

```
develop  <--PR--  feat/... fix/... chore/...
qa       <--PR--  qa/...
main     <--PR--  release/...  hotfix/...
```

Promotion happens **by re-application** (`git cherry-pick -x`), never by merging one permanent
branch into another: `merge develop -> qa` and `merge qa -> main` do not exist in this model.

`main` requires **1 approval from `ariel5253`**. On `develop` and `qa` the team sets its own review
rule.

Full policy: `00-governance/branching-policy.md` in `multi-tour-docs`.
