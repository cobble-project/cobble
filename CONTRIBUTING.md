# Contributing

This project is a Rust \+ Cargo codebase. Use the sections below to set up your environment, run tests, and format PR titles consistently.

## 1\. Setup

### 1\.1 Prerequisites

- Rust toolchain (stable)
- Cargo

Recommended Rust components:
- `rustfmt`
- `clippy`

Install Rust (if needed):
- Follow https://rustup.rs

Install components:
- `rustup component add rustfmt clippy`

### 1\.2 Build

Debug build:
- `cargo build`

Release build:
- `cargo build --release`

### 1\.3 Format and Lint

Format:
- `cargo fmt`

Lint (treat warnings as errors):
- `cargo clippy --workspace -- -D warnings`

Fix lint issues automatically (where possible):
- `cargo clippy --fix --allow-dirty --allow-staged`

## 2\. Testing

### 2\.1 Run all tests

- `cargo test --workspace`

### 2\.2 Run a subset of tests

Run tests matching a name pattern (example):
- `cargo test row_codec`

### 2\.3 Notes on filesystem\-based tests

Some tests may write temporary files under `/tmp` and may use serial execution attributes to avoid conflicts. If you see intermittent failures, remove stale temp directories under `/tmp` from prior runs.

## 3\. PR Title Format

Use the following format (Conventional Commits style):

```text
<type>: <summary>
```

### 3\.1 Allowed type
* feat: new feature
* fix: bug fix
* perf: performance improvement
* refactor: code change that neither fixes a bug nor adds a feature
* test: add or update tests
* docs: documentation only
* chore: tooling, build, deps, or non-code changes
