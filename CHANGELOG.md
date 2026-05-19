# Changelog

All notable changes to this crate are documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `script_arg.instance: { id, step }` — workflow scripts now
  receive the running `WorkflowInstance`'s public V4 UUID
  (string) and the step seq (number, 1-based) currently
  executing. Non-breaking: scripts that don't read
  `arg.instance` are unaffected.

### Changed
- **Breaking (script-arg shape):** `subject.tenant_id` and
  `subject.authority_id` are now bare public V4 UUID strings
  (or `null` for `authority_id` on principal callers), not
  `{internal, public}` objects. The previous nested shape was
  a serde default leak of the internal V7 UUID into JS
  scripts; the public V4 is the only identity scripts should
  observe. Scripts that read `arg.subject.tenant_id.public`
  must now read `arg.subject.tenant_id` directly. The
  persisted `StepRecordSubject` (audit shape) is unchanged.

## [0.1.6] - 2026-05-14

### Changed
- Internal Cargo.toml audit: `default-features = false` set on
  direct dependencies with explicit feature lists for what the
  crate actually uses. No behaviour change. (D24)

## [0.1.5] - 2026-05-13

- Dev: migrate integration-test fixtures from `testcontainers` /
  `testcontainers-modules` to `dockerlet 0.1`. No public-API
  change; runtime behaviour unchanged.

## [0.1.4] - 2026-05-10

- Added workflow-engine `data.embed_datasets` script argument assembly
  from template `data_config` bindings.
- Added `WorkflowError::DataConfigInvalid` for malformed or unsafe
  template data bindings.

## [0.1.3] - 2026-05-10

- Added the optional `data_config` content slot to
  `WorkflowTemplate.CONTENT_SLOTS`.

## [0.1.2]

- Added doc comments on `WorkflowError` variant fields.

## [0.1.1]

## [0.1.0] - 2026-04-22

### Added

- Initial `philharmonic-workflow` implementation for Phase 4.
- Entity kinds: `WorkflowTemplate`, `WorkflowInstance`, and `StepRecord`.
- `SubjectContext` and `SubjectKind`, reusing `philharmonic-policy`
  markers (`Tenant`, `MintingAuthority`).
- Async trait boundaries: `StepExecutor` and `ConfigLowerer`.
- `WorkflowEngine<S, E, L>` with `create_instance`, `execute_step`,
  `complete`, and `cancel`.
- Five-state lifecycle handling with terminal-state immutability checks.
- Nine-step execution sequence implementation, including step-record-first
  write ordering and malformed-result handling.
- Audit-discipline enforcement for `StepRecord.subject`: the persisted
  shape is a `StepRecordSubject` newtype carrying only `kind`, `id`,
  and `authority_id`. `claims` and `tenant_id` are structurally absent
  from the persisted type, so they cannot be leaked into a step record
  even by accident. Backed by a behavioral test.
- Two-tier tests:
  - Tier 1: always-on mock substrate integration tests (8 tests).
  - Tier 2: `#[ignore]` MySQL testcontainers integration tests (3 tests).
- Verified clean under `cargo +nightly miri test` for the full tier-1
  test suite prior to publish.

## [0.0.0]

Name reservation on crates.io. No functional content yet.
