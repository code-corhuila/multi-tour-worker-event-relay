# multi-tour-worker-event-relay

> Worker job 7: event relay between domains (outbox delivery)

Part of the **Multi Tour** distributed system — team `multi-tour`, Grupo 2.
Governance and documentation live in [`multi-tour-docs`](https://github.com/code-corhuila/multi-tour-docs).

## What it is

Worker job 7 (ADR-004 D3, D10): it reads the outbox of each publishing domain through its internal API and delivers every event to the consumers listed in its routing table. It reads four outboxes: `identity-audit`, `security`, `operations-cost` and `commercial-reservation`. The tenant events reach `security` and `commercial-reservation`; `AccountEmailVerified` reaches `commercial-reservation` (it links the account to the customer of that tenant) and the audit; and the 14 events that `commercial-reservation` writes reach the audit (relay spec 007). What is not routed yet: `cash-reporting` (its outbox has no route to read it), `AccountSessionsRevoked` towards the domains, and the tenant events towards `cash-reporting` and `operations-cost`, because none of them has a receiving route. It holds no business rule and exposes only `/health`.

## The service token

The relay calls the internal routes of every service with `Authorization: Bearer $SERVICE_TOKEN`, a token of role `SERVICE` that `multi-tour-security` issues for it:

```
security issue-service-token worker-event-relay 720h
```

(`sub` is `worker-event-relay`; the duration goes from one minute to 30 days.) In `develop`, `multi-tour-infra-postgres/scripts/dev-token.sh worker-event-relay` runs that same command. In `qa` and `main` the value is a secret of the environment, set in `env/qa.env` or `env/main.env`: never in a compose file, an image or the repository, and renewed before it expires (the relay answers `401` to every call once it has).

## How to start it

Copy `.env.example` to `.env` and set `SERVICE_TOKEN`. Then either run it with Go 1.26, exporting the variables of `.env` first:

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
| `SECURITY_BASE_URL` | none, required (e.g. `http://security:8086`; compose also requires it) | Internal base URL of `multi-tour-security` (publisher and recipient) |
| `IDENTITY_AUDIT_BASE_URL` | none, required (e.g. `http://identity-audit-api:8082`; compose also requires it) | Internal base URL of `multi-tour-identity-audit-api` (publisher) |
| `OPERATIONS_COST_BASE_URL` | none, required (e.g. `http://operations-cost-api:8084`; compose also requires it) | Internal base URL of `multi-tour-operations-cost-api` (publisher: its scheduled and rescheduled departures) |
| `COMMERCIAL_RESERVATION_BASE_URL` | none, required (e.g. `http://commercial-reservation-api:8083`; compose also requires it) | Internal base URL of `multi-tour-commercial-reservation-api` (consumer of `SalidaScheduled`, `SalidaVehiclesChanged`, `SalidaExecutionStarted` and `SalidaFinalized`) |
| `NOTIFICATIONS_BASE_URL` | none, required (e.g. `http://worker-notifications:8080`; compose also requires it) | Internal base URL of `multi-tour-worker-notifications` (consumer of `PasswordReset` and `PasswordResetFailed`, which it tells the owner of an administrative account about) |
| `RELAY_INTERVAL` | `5s` | Time between two runs |
| `BATCH_SIZE` | `100` | Events handled at most per run |
| `RUN_TIMEOUT` | `30s` | Time limit of one run |
| `HTTP_TIMEOUT` | `5s` | Time limit of each HTTP attempt |
| `HTTP_ATTEMPTS` | `3` | Attempts per call (network, `429` and `5xx` only) |
| `HEALTH_ADDR` | `:8080` | Where `GET /health` listens (compose passes it through; the healthcheck and `expose` assume 8080) |

`GET /health` only says the process is up: it does not reflect the runner or the last run. A base URL holds only scheme, host and port (no credentials, path, query or fragment).

The routing table (which event type goes to which service) lives in `internal/config/routing.go`.

## Where the data is

Nowhere: it keeps no database and no state. Each event stays in the outbox of its publisher.

## How to test it

`gofmt -l .` must print nothing, then `go vet ./...` and `go test ./...` (Go 1.26). The same three run in `ci.yml` on pull requests to `develop` and `qa`.

## What is missing

`multi-tour-worker-runtime` is pinned to a commit of its `develop` (pseudo-version in `go.mod`) until it is tagged. The six security events that the audit records (`AccountSessionsRevoked`, `AccountBlocked`, `SpecialPermissionGranted`, `SpecialPermissionRevoked`, `OptionalRoleEnabled` and `OptionalRoleDisabled`) go to identity-audit, by the path of the tenant or, for an account that belongs to none, by the path without tenant. `AccountTemporaryCredentialIssued` and `AccountEmailVerified` have no consumer yet, so they are marked published without delivery. The Docker image is built by hand or by the root composition; `ci.yml` only validates the compose file.

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

`SalidaScheduled` of `multi-tour-operations-cost-api` goes to `multi-tour-commercial-reservation-api`, which creates the capacity row of the departure from it (HU-OPS-001, AC5). `SalidaVehiclesChanged` takes the same route, so that service can lower its ceiling to the capacity of the vehicles (HU-OPS-002, AC5; her consumer was tested apart, not together with the relay). `SalidaExecutionStarted` and `SalidaFinalized` take the same route, so that service learns that a departure started and ended (HU-OPS-003); `multi-tour-operations-cost-api` writes them when a person records the start or the end (HU-OPS-003 part A), and both were seen reaching the route on the `dev` stack. `SalidaRescheduled` is known and has no consumer: commercial-reservation does not accept it yet (gap G-API-2 of spec 002 of the operations-cost API), so it is published without delivery; when it does, it takes the same route as `SalidaScheduled`. `SalidaMarkedPendienteDeRegularizacion` (written by `multi-tour-operations-cost-api` when job 3 marks a departure past its estimated end, HU-OPS-003 part B) is known and also has no consumer yet, so it is published without delivery too. The relay reads page 1 of each outbox until it is empty, so the events it marks published do not shift the pages (the outbox of operations-cost lists the events that are due, oldest first).
