# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GTSDB is a Golang-based Dead Simple Timeseries Database that uses a fundamentally different approach from traditional databases. Instead of using WAL + disk blocks, it relies primarily on WAL for everything, saving IO and memory usage. The database supports both HTTP and TCP protocols for client communication.

## Repository Layout

The repo contains the main server plus several submodules (separate GitHub repos):

- **Root** — the GTSDB server itself (this codebase)
- **`auth/`** — multi-user token authentication and `data/users.json` persistence
- **`quota/`** — per-user storage quota enforcement (cached counters + background reconciler)
- **`admin/`** (submodule) — Next.js admin console
- **`cloud/`** (submodule) — managed cloud portal (spawns the server binary)
- **`homepage/`** (submodule) — marketing site
- **`drivers/`** (submodule) — Go and Node.js client drivers
- **`benchmark/`** (submodule) — GTSDB vs InfluxDB/VM/NSQ benchmark tooling

Initialize submodules with `make submodules` (or `git submodule update --init --recursive`).

## Architecture

### Core Components
- **main.go**: Entry point, starts both HTTP and TCP servers with graceful shutdown
- **handlers/**: HTTP and TCP request handlers with common operation logic
- **buffer/**: Data buffering and persistence layer with external/internal operations
- **fanout/**: Publisher-subscriber notification system for real-time updates
- **auth/**: Token-based multi-user authentication (root token, namespaced keys)
- **quota/**: Per-user storage quotas (O(1) write-path check, periodic reconciler)
- **utils/**: Shared utilities including configuration and data directory management
- **concurrent/**: Thread-safe data structures (maps, sets, LRU cache)
- **synchronous/**: Ring buffer implementation
- **models/**: Data structure definitions

### File Handle Lifecycle (IMPORTANT — read before editing `buffer/`)

Open file handles are cached in two LRUs (`dataFileHandles`, `indexFileHandles` in `buffer/states.go`),
bounded by `file_handle_lru_capacity` (default 700). To prevent use-after-close races, handles are
reference-counted:

- `acquireFileHandle(...)` / `refFromLRU(...)` return a `*refFile` with a reference held.
- **Every successful acquire MUST be released with `defer ref.release()` registered on the same
  path as the acquire** — either the if-init form (`if ref, ok := acquire(...); ok { defer ref.release() ... }`)
  or assign + immediately-following defer (only `if !ok { return }` guards allowed in between).
- `TestAcquireReleasePairing` in `buffer/refcheck_test.go` **statically enforces this on the package
  source** — a new call site that forgets release, or registers it conditionally/after an early
  return, fails `go test ./buffer/`. Keep new acquire sites in one of the two accepted forms.
- Reads use `ReadAt` (position-independent); do not add `Seek`+read patterns on shared handles.
- `primeFileHandle(...)` warms the cache without holding a reference — use it for open-and-forget.

### Key Design Principles
- WAL-first approach: Write data to WAL, create indexes conditionally, read from WAL and indexes
- Dual protocol support: HTTP (port :5556) and TCP (port :5555) servers
- Real-time subscriptions via fanout mechanism (writes publish to subscribers with resolved timestamps)
- High concurrency with custom thread-safe data structures
- Goroutines never read mutable config globals after startup — pass values (addr, noAuthUser) as parameters

## Commands

### Build and Run
```bash
go run .                    # Run the database server
go build .                  # Build binary
./gtsdb custom.ini          # Run with a custom config file
```

### Testing
```bash
go test ./...               # Run all tests
go test ./... -count=1      # Run without cache
make GenerateTest           # Generate coverage report (docs/coverage + docs/coverage.html)
make integration-test       # Run integration tests (-tags=integration)
```
CI runs `go test -race ./...` — the race detector cannot run on Windows locally (needs gcc),
so check CI results for race findings.

### Benchmarking
```bash
go test -benchmem -run=^$ -bench ^BenchmarkMain$ -benchtime=5s     # Main benchmark (50% read/write ops)
make Benchmark              # Concurrent data structures benchmarks
```

### Code Quality
```bash
golangci-lint run           # Run linter (errcheck is enabled — keep it green)
make lint                   # Run linter via Makefile
make lint-fix               # Run linter with auto-fix via Makefile
```

### Submodules
```bash
make submodules             # Initialize all submodules
```

## Configuration

The application uses INI files for configuration (default: `gtsdb.ini`, example: `gtsdb.ini.example`):

- `[listens]` — `tcp` and `http` listen addresses
- `[paths]` — `data` directory path
- `[buffer]` — `file_handle_lru_capacity` (default 700), `compaction_compression` (Gorilla),
  `sync_mode` ("async"/"sync"), `sync_interval_ms` (default 1000), `cache_size` (ring buffer, 0 = off)
- `[auth]` — `no_auth_user` (skip auth for a user), `root_token` (fixed root token;
  auto-generated on first start if empty)
- Configuration file can be specified as command line argument: `./gtsdb custom.ini`

## API Operations

The database supports these operations via HTTP POST `/` (Bearer token auth) and the TCP protocol:

- **Data**: `write`, `batch-write`, `read`, `multi-read`, `export`, `data-patch`, `deleteDataPoint`
- **Keys**: `ids`, `idswithcount`, `initkey`, `renamekey`, `deletekey`, `reloadkey`, `compact`
- **Realtime**: `subscribe`/`unsubscribe` (SSE on HTTP, push on TCP)
- **Admin**: `adduser`, `resetkey`, `setquota` (root only)
- **Other**: `flush`, `serverinfo`
- **Monitoring (no auth)**: `GET /health`, `GET /metrics`

See `docs/openapi.yaml`, `docs/operations.md`, and `docs/tcp-protocol.md` for details.

## Testing Strategy

The codebase maintains high test coverage with:
- Unit tests for all major components
- Integration tests in `integration_test.go` (build tag `integration`, run via `make integration-test`)
- `buffer/refcheck_test.go` — static AST enforcement of the file-handle acquire/release contract
- `buffer/lru_filehandle_test.go` — refcount balance and eviction semantics tests
- Benchmark tests for performance validation (`*_bench_test.go`)
- Concurrent data structure specific tests
- End-to-end HTTP/TCP handler tests (`handlers/`)
