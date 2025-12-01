# Kagami

Actor-model distributed mirror system with a central Supervisor coordinating stateless Workers over NATS/JetStream. Each Worker hosts multiple replicas (git/rsync/http) and reports health and status; the Supervisor aggregates state, exposes an HTTP API plus a Vue-based UI at `/ui/`, and persists snapshots via SeaORM (SQLite by default).

## Components
- **Supervisor**: subscribes to worker join/heartbeat, approves/rejects/terminates workers, sends sync/remove/add-replica commands, serves API+UI, persists state.
- **Worker**: loads providers from `worker.toml` or `KAGAMI_*` env, sends join/heartbeat, listens for commands, exposes `/ping` and `/metrics`.
- **CLI (`kagami-cli`)**: helper for docker build/run and supervisor approvals/terminations.

## Prerequisites
- Rust toolchain (nightly not required), `cargo`.
- NATS reachable by both Supervisor and Workers.
- For Docker deployment: Docker Engine, and a volume for the supervisor SQLite DB if persistence is required.

## Quickstart (kagami-cli)
Use the CLI to build and run everything (requires Docker installed and running):
```bash
cargo run -p kagami-cli -- docker build worker --supervisor-tag kagami-supervisor --worker-tag kagami-worker
cargo run -p kagami-cli -- docker run nats --port 4222 --monitor-port 8222
cargo run -p kagami-cli -- docker run supervisor \
  --image kagami-supervisor \
  --nats-url nats://host.docker.internal:4222 \
  --http-addr 0.0.0.0:21000 \
  --db-url sqlite:///data/supervisor.db?mode=rwc \
  --auto-approve false \
  --port 21000
cargo run -p kagami-cli -- docker run worker \
  --image kagami-worker \
  --nats-url nats://host.docker.internal:4222 \
  --data-dir /var/lib/kagami/worker \
  --port 8080 \
  --config ./worker.toml
```
Notes:
- The CLI uses `host.docker.internal` to reach NATS from inside containers on macOS/Windows; adjust to your host IP as needed.
- Mount a host volume (`/var/lib/kagami:/data`) for supervisor DB persistence (see Docker example below).

## Configuration
### Worker (`worker.toml` or env)
`providers` supports detailed objects:
```toml
[[providers]]
name = "repo-mirror"
kind = "git"            # protocol: git/rsync/http
upstream = "https://example.com/repo.git"
interval_secs = 300     # 0 or omit for manual-only
labels.env = "prod"
labels.region = "us"

# NATS address (defaults to nats://127.0.0.1:4222)
# nats_url = "nats://nats:4222"
```
Env overrides (prefix `KAGAMI_`): `KAGAMI_NATS_URL`, `KAGAMI_PROVIDERS` (JSON array), etc.
- `data_dir` (required): where synced artifacts are written; worker will fail to start if omitted. Set via config, or env `KAGAMI_DATA_DIR`.
- Sync behavior:
  - `git`: `git clone --mirror` on first run, then `git fetch --all --prune`.
  - `rsync`: `rsync -az --delete <upstream> <data_dir>/<name>/`.
  - `http`: downloads the target URL into `<data_dir>/<name>/<filename or index.html>`.
  - Make sure the corresponding tools (`git`, `rsync`) are available when using those kinds.

### Supervisor env
- `KAGAMI_NATS_URL` (required): NATS address.
- `SUPERVISOR_HTTP_ADDR` (default `0.0.0.0:21000`).
- `SUPERVISOR_DATABASE_URL` (default `sqlite://supervisor.db?mode=rwc`). Mount a volume when running in Docker to persist.
- `SUPERVISOR_AUTO_APPROVE` (`true`/`false`, default `true`).
- `SUPERVISOR_WEBUI_DIR` (optional override for UI assets).

## Deployment (Docker)
Prefer using `kagami-cli` (above) to build/run. For manual reference only:
- Build: `docker build -f Dockerfile.supervisor -t kagami-supervisor .` and `docker build -f Dockerfile.worker -t kagami-worker .`
- Run supervisor with DB volume: `docker run -d --rm -p 21000:21000 -e ... -v /var/lib/kagami:/data kagami-supervisor`
- Run worker with config mount: `docker run -d --rm -e KAGAMI_NATS_URL=... -v /path/to/worker.toml:/app/worker.toml:ro kagami-worker`

## HTTP API (Supervisor)
- `GET /overview` – fleet summary.
- `GET /workers` – workers + replicas.
- `POST /workers/{id}/approve` – approve pending worker.
- `POST /workers/{id}/reject` – reject pending worker.
- `DELETE /workers/{id}` – terminate worker.
- `POST /workers/{id}/sync` – body: `{ "resource": "name" | null }`.
- `POST /workers/{id}/remove_replica` – body: `{ "replica_id": "..." }`.
- `POST /workers/{id}/add_replica` – body:
  ```json
  {
    "name": "repo-mirror",
    "kind": "git",
    "upstream": "https://example.com/repo.git",
    "interval_secs": 300,
    "labels": { "env": "prod" }
  }
  ```
- `GET /resources` – aggregated resources/replicas.
- UI: `GET /ui/` (static Vue bundle).

## Observability
- Worker: `/ping` health, `/metrics` Prometheus exposition.
- Logs: uses `tracing`; run with `RUST_LOG=info` as needed.

## Development
- Build: `cargo build --workspace`
- Format: `cargo fmt --all`
- Lint: `cargo clippy --workspace --all-targets --all-features`
- Test: `cargo test --workspace`

## Notes
- Default DB is SQLite in the supervisor working directory; mount a volume for persistence in Docker.
- Workers continuously retry join until approved or rejected; rejected workers stop themselves.
- SeaORM is pinned to `1.1.19`; SQLx runtime via `runtime-tokio-rustls`.
