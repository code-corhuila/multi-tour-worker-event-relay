# Changelog

All notable changes of `multi-tour-worker-event-relay` (Event relay worker) are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [3.1.0] - 2026-11-18

Cut 3 follow-up release. The entries below are the commits of pull request 54 merged into `qa` and re-applied to `release/3.1.0` with `git cherry-pick -x`. The frozen head of `qa` is `d5b2caf5a`.

### Changed

#### Cross-cutting: pull request template (no single story is named in the pull request title, branch or body)

- The repository has a pull request template ([PR 54](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/54))


## [3.0.0] - 2026-11-14

Cut 3 release. Every entry below is a pull request merged into `qa` and re-applied to `release/3.0.0` with `git cherry-pick -x`; the heading of each group is the user story named in the pull request, and the entry is the title of the pull request without its type. The frozen head of `qa` is `a1c0734ff23010ef3d779e5a7c1fec1e34559456`.

### Added

#### HU-IDA-004 - Change the time zone and the country of the tenant

- Publish the change of localization of a tenant without delivery ([PR 34](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/34))

### Changed

#### HU-COM-015 - Notify the Final Customer

- The routes of the catalog, credit and reschedule events ([PR 38](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/38))

#### HU-SEC-005 - Manage the tenant's internal users

- Send the revocation of the sessions to commercial reservation and cash reporting ([PR 46](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/46))
- Send the revocation of the sessions to operations-cost ([PR 49](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/49))

#### HU-WRK-007 - Relay events between domains

- The relay routes and alerts (part 1) ([PR 31](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/31))
- The outbox reads and the access notices (part 2) ([PR 32](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/32))
- HU-WRK-007 ([PR 47](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/47))

#### HU-WRT-001 - Share the worker runner as a versioned module

- Pin the worker runtime at its first version v0.1.0 ([PR 51](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/51))

#### Cross-cutting: CI, test fixes and alignment promotions (no single story is named in the PR title, branch or body)

- The cancellation, cost and cash-reporting routes ([PR 42](https://github.com/code-corhuila/multi-tour-worker-event-relay/pull/42))

[Unreleased]: https://github.com/code-corhuila/multi-tour-worker-event-relay/compare/v3.0.0...HEAD
[3.0.0]: https://github.com/code-corhuila/multi-tour-worker-event-relay/releases/tag/v3.0.0
