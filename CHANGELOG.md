# Changelog

All notable changes to this crate are documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- Audit-discipline enforcement for `StepRecord.subject` (claims are never persisted).
- Two-tier tests:
  - Tier 1: always-on mock substrate integration tests.
  - Tier 2: `#[ignore]` MySQL testcontainers integration tests.

## [0.0.0]

Name reservation on crates.io. No functional content yet.
