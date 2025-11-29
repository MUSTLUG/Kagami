# Repository Guidelines

## Project Structure & Module Organization
- Workspace root uses `Cargo.toml` with shared dependencies; build artifacts live under `target/`.
- `crates/common`: shared data types and constants (e.g., `Heartbeat`, `HB_SUBJECT_PREFIX`).
- `crates/worker`: Actix-based worker binary that connects to NATS/JetStream, sends heartbeats, and serves `/ping` and `/metrics`; configuration defaults from `worker.toml` and `KAGAMI_*` env vars.
- `crates/supervisor`: starting point for coordination logic; currently a minimal crate with sample tests.
- `worker.toml` serves as a local template for provider lists and optional `nats_url`.

## Build, Test, and Development Commands
- `cargo build --workspace` — compile all crates.
- `cargo fmt --all` — format with rustfmt defaults.
- `cargo clippy --workspace --all-targets --all-features` — lint; fix warnings before submitting.
- `cargo test --workspace` — run unit/integration tests across crates.
- `cargo run -p worker` — start the worker; override config with `KAGAMI_NATS_URL` or edit `worker.toml`.

## Coding Style & Naming Conventions
- Rust 2024 edition; keep imports ordered, 4-space indentation, and rely on `cargo fmt`.
- Prefer `snake_case` for functions/modules, `PascalCase` for types, and `SCREAMING_SNAKE_CASE` for constants.
- Use `tracing` for structured logs and `anyhow` for error contexts; propagate errors instead of panicking in async paths.
- Keep shared types in `crates/common`; avoid duplicating heartbeat schema elsewhere.

## Testing Guidelines
- Co-locate unit tests with modules via `#[cfg(test)] mod tests` or add integration tests under `tests/`.
- Use descriptive test names (`publishes_heartbeat_on_interval`) and table-driven cases when feasible.
- For async/actor code, use `#[actix_rt::test]` or spawn test runtimes explicitly to avoid timer flakiness.
- Add tests when touching message handling, NATS publishing, or metrics endpoints; ensure `cargo test --workspace` passes.

## Commit & Pull Request Guidelines
- No established history yet; follow Conventional Commits (e.g., `feat: add worker heartbeat metrics`) with imperative, ≤72-char subjects.
- Keep PRs focused; include a short summary, manual test commands executed, and linked issues/tasks.
- Call out config changes (`worker.toml`, env expectations), breaking behavior, and any required NATS/local stack steps.
- Add screenshots or logs when modifying HTTP endpoints or metrics output for easier review.

## Configuration & Operational Notes
- Default config reads `worker.toml`; environment overrides use the `KAGAMI_` prefix (`KAGAMI_PROVIDERS`, `KAGAMI_NATS_URL`).
- The worker listens on `0.0.0.0:8080`; ensure NATS is reachable before running locally.
- Do not commit secrets or host-specific settings; keep local overrides in untracked files.
