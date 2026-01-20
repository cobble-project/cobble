# Agent guidelines for stellakv project

This repository is a Rust + Cargo project implementing an LSM-style KV storage engine with SST (Sorted String Table) files.

## Goals
- Make small, focused, and reviewable changes.
- Keep build, formatting, linting, and tests passing.
- Follow the repository's PR title and description conventions.

## How to Test
- Run all tests: `cargo test`
- Run a subset: `cargo test <pattern>` (e.g., `cargo test row_codec`)
  Notes:
- Some tests write temporary files under `/tmp` and may use serial execution attributes. Remove stale `/tmp` test artifacts if tests intermittently fail.

## How to Lint & Format
- Format: `cargo fmt`
- Lint (treat warnings as errors): `cargo clippy -- -D warnings`
- Optional auto-fix for some lints: `cargo clippy --fix --allow-dirty --allow-staged`

Before opening a PR, ensure:
- `cargo fmt` produces no diffs
- `cargo clippy -- -D warnings` passes
- `cargo test` passes locally

## PR Title Format
Use this exact format for PR titles:
`<type>: <summary>`

Allowed `type` values:
- `feat` — new feature
- `fix` — bug fix
- `perf` — performance improvement
- `refactor` — refactor without behavior change
- `test` — add or update tests
- `docs` — documentation only
- `chore` — tooling, build, deps, non-code work
