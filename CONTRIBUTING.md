# Contributing to Tine

Tine accepts contributions from humans and from AI agents working on narrow, testable changes under the constraints in this guide.

The highest quality contributions in this repo are scoped, testable, and aligned with the current product model: one local-first backend shared by the UI and MCP.

This document is meant to answer four questions quickly:

1. what the repo needs right now
2. which issues are good for AI agents vs humans
3. which subsystem owns a problem
4. which validation step should be run before proposing a change

## Core rules

- Preserve the local-first product model.
- Keep the tree-native execution model intact.
- Prefer narrow fixes over broad refactors.
- Do not mix unrelated cleanup into behavior changes.
- Update docs when public behavior changes.
- Keep version bumps and release steps separate from functional changes when possible.

## What the repo needs now

If you want to contribute, start from the current needs of the codebase rather than from generic cleanup.

The highest-value work right now is:

1. frontend execution-state fidelity
2. runtime hardening for restart, replay, and failure paths
3. targeted e2e coverage for failure and recovery paths
4. release observability and wrapper reliability
5. MCP contract polish after the above

In more concrete terms, the repo needs help with:

- making the UI reflect backend execution truth more reliably
- tightening failure-path behavior after kernel loss, restart, or replay
- adding regression coverage for real branch-aware notebook flows
- making release and wrapper behavior easier to verify and reason about
- improving MCP stability without expanding scope too broadly

The repo does not need broad redesign work as a default contribution path.

## Issue tags

Use tags as the preferred maintainer triage convention for routing and pickup. Contributors may see these labels on issues, but the guidance in this document still applies when an issue has not been labeled yet.

Recommended priority tags:

- `priority:now` - directly aligned with the current repo needs above
- `priority:next` - valuable follow-up after the current priority set
- `priority:later` - useful, but not on the immediate path

Recommended ownership tags:

- `agent:good-first-task` - narrow, testable, and safe for an AI agent to handle
- `agent:needs-human` - requires product judgment, architectural decisions, or release authority
- `human:maintainer` - should stay with a maintainer or a deeply familiar contributor

Recommended subsystem tags:

- `area:runtime` - tree-native execution, replay, runtime state, branch context
- `area:kernel` - kernel lifecycle, heartbeats, restart, execution timeout, recovery
- `area:scheduler` - execution orchestration, retries, status flow
- `area:server` - REST routes, WebSocket surface, request validation
- `area:ui` - notebook state, hydration, polling, rendering, branch-aware identity
- `area:mcp` - MCP payloads, config generation, protocol-facing behavior
- `area:wrapper` - Python wrapper, engine resolution, package/runtime matching
- `area:release` - CI, release artifacts, version alignment, PyPI publishing
- `area:docs` - user-facing and contributor-facing documentation
- `area:tests` - regression coverage, fixture improvements, failure-path tests

Recommended task-shape tags:

- `type:bug`
- `type:regression-test`
- `type:hardening`
- `type:docs`
- `type:release`
- `type:validation`

## How to pick up work

If you are a human contributor:

- prefer issues tagged `priority:now` when labels are present
- prefer issues with one `area:*` tag and one `type:*` tag
- escalate issues tagged `agent:needs-human` if they involve architecture, product scope, or release actions

If you are contributing through an AI agent:

- prefer issues tagged `agent:good-first-task` when labels are present
- prefer exactly one subsystem tag such as `area:runtime` or `area:release`
- prefer issues with a clear validation target such as `cargo test -p tine-api --test e2e`
- avoid issues that require product design, release authority, or multi-subsystem rewrites

## Good issues for AI agents

AI agents are most effective on issues that are small enough to verify locally and narrow enough to reason about in one pass.

The best issue areas for agents in this repo are:

- tree execution correctness and replay regressions
- branch-aware runtime state and log hydration fixes
- MCP request normalization and config generation
- packaging and release automation fixes
- Python wrapper compatibility and binary resolution
- focused frontend reliability fixes in polling, hydration, and execution state
- docs that track shipped behavior

Good issue shapes:

- add or fix a regression test for branch execution behavior
- fix a narrow runtime bug with an e2e test
- tighten one API or MCP payload validation path
- improve one release verification or wrapper smoke check
- update one doc section after a user-facing command, route, or runtime default changed

Strong examples of agent-friendly issues in this repo:

- `priority:now area:ui type:bug agent:good-first-task`
  Fix polling and status reconciliation for one execution-state edge case.
- `priority:now area:tests type:regression-test agent:good-first-task`
  Add an e2e test for one replay, runtime-state, or restart regression.
- `priority:next area:mcp type:hardening agent:good-first-task`
  Tighten one MCP payload normalization or config generation path.
- `priority:next area:release type:validation agent:good-first-task`
  Improve one release verification or wrapper smoke check.

## Work that should stay human-led

Avoid assigning the following as default agent work:

- broad product redesigns
- large UI rewrites without a fixed spec
- multi-crate refactors without explicit acceptance criteria
- architecture changes that alter the local-first model
- remote release or publish actions without explicit human approval
- ambiguous work with no clear validation target

Typical human-led issues:

- `agent:needs-human area:ui`
  product-level interaction or layout redesign
- `agent:needs-human area:runtime`
  changes to the tree-native execution contract
- `human:maintainer area:release`
  version bumps, tagging, release publication, or rollback decisions

## Where to contribute

The main code ownership areas are:

- `crates/tine-api/`: tree-native workspace logic and correctness boundary
- `crates/tine-kernel/`: kernel lifecycle, heartbeats, restart, execution timeouts
- `crates/tine-scheduler/`: execution orchestration and retry behavior
- `crates/tine-server/`: REST and WebSocket surface
- `ui/`: notebook UI, execution polling, output hydration, branch-aware state
- `packaging/python/`: Python wrapper, MCP entrypoints, binary resolution, release packaging

Use this map when choosing an issue:

- if the bug is about branch replay, runtime materialization, or tree execution semantics, start in `crates/tine-api/`
- if the bug is about kernel startup, timeout, restart, or liveness, start in `crates/tine-kernel/`
- if the bug is about execution retries or node orchestration, start in `crates/tine-scheduler/`
- if the bug is about HTTP payloads or route behavior, start in `crates/tine-server/`
- if the bug is about notebook status, output hydration, or branch-aware rendering, start in `ui/`
- if the bug is about installed-package behavior or release binary lookup, start in `packaging/python/`

## Validation guide

Use the smallest relevant validation step for the change you made.

- general Rust work: `cargo test`
- tree execution and runtime changes: `cargo test -p tine-api --test e2e`
- Python wrapper changes: `python -m unittest discover -s packaging/python/tests`
- release checks: `python3 scripts/release/verify_version_alignment.py --repo-root .`

If a change touches runtime execution and release packaging, run both the e2e suite and the release validation step.

Recommended validation by tag:

- `area:runtime`, `area:kernel`, `area:scheduler`, `area:tests`:
  run `cargo test -p tine-api --test e2e`
- `area:wrapper`:
  run `python -m unittest discover -s packaging/python/tests`
- `area:release`:
  run `python3 scripts/release/verify_version_alignment.py --repo-root .`
- `area:docs`:
  verify the docs match current commands, routes, or behavior

## Current priorities

The current best contribution targets are:

1. frontend execution-state fidelity
2. runtime hardening for restart, replay, and failure paths
3. targeted e2e coverage for failure and recovery paths
4. release observability and wrapper reliability
5. MCP contract polish after the above

If you are triaging issues, prefer filing them with tags that make this obvious, for example:

- `priority:now area:ui type:bug agent:good-first-task`
- `priority:now area:tests type:regression-test agent:good-first-task`
- `priority:next area:release type:validation agent:good-first-task`
- `priority:next area:mcp type:hardening agent:good-first-task`

## Suggested contribution workflow

1. Pick one narrow issue with a clear acceptance condition.
2. Find the single subsystem that owns the behavior.
3. Make the smallest change that fixes the problem at the root.
4. Run the narrowest relevant validation.
5. Update docs if user-facing behavior changed.

That workflow works well for both humans and agents, and it matches how this repo is easiest to maintain.