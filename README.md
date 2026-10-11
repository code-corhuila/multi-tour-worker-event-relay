# multi-tour-worker-event-relay

> Worker job 7: event relay between domains (outbox delivery)

Part of the **Multi Tour** distributed system — team `multi-tour`, Grupo 2.
Governance and documentation live in [`multi-tour-docs`](https://github.com/code-corhuila/multi-tour-docs).

## What it is

Worker job 7 (ADR-004 D3, D10): it reads the outbox of each publishing domain through its internal API and delivers every event to the consumers listed in its routing table. It holds no business rule and exposes only `/health`.

## How to start it

Not runnable yet. Copy `.env.example` to `.env`, set `SERVICE_TOKEN` (issued by the identity service) and, once the relay exists, build it from `cmd/worker` (Go 1.26).

## Where the data is

Nowhere: it keeps no database and no state. Each event stays in the outbox of its publisher.

## How to test it

`gofmt -l .` must print nothing, then `go vet ./...` and `go test ./...` (Go 1.26). The same three run in `ci.yml` on pull requests to `develop` and `qa`.

## What is missing

The relay logic is not built yet: today the packages only hold `doc.go`. The module does not yet require `multi-tour-worker-runtime` (not merged or tagged); T-02 onwards need it. Still to come: the tests, the routing table and use case, and the image and compose file.

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
